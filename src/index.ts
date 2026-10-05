import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema, ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import express, { type Request, type Response } from 'express';
import { timingSafeEqual, randomUUID } from 'node:crypto';
import { WhoopClient } from './whoop-client.js';
import { WhoopDatabase } from './database.js';
import { WhoopSync } from './sync.js';

interface ToolArguments {
	days?: number;
	full?: boolean;
}

const config = {
	clientId: process.env.WHOOP_CLIENT_ID ?? '',
	clientSecret: process.env.WHOOP_CLIENT_SECRET ?? '',
	redirectUri: process.env.WHOOP_REDIRECT_URI ?? 'http://localhost:3000/callback',
	dbPath: process.env.DB_PATH ?? './whoop.db',
	port: Number.parseInt(process.env.PORT ?? '3000', 10),
	mode: process.env.MCP_MODE ?? 'http',
	// Time zone used to label days and times (WHOOP stores everything in UTC).
	timezone: process.env.WHOOP_TIMEZONE ?? 'Europe/Paris',
	// Optional secret: when set, the MCP endpoint is only reachable at /mcp/<MCP_ACCESS_KEY>.
	accessKey: process.env.MCP_ACCESS_KEY ?? '',
};

const OAUTH_SCOPES = ['read:profile', 'read:body_measurement', 'read:cycles', 'read:recovery', 'read:sleep', 'read:workout', 'offline'];

const db = new WhoopDatabase(config.dbPath);
const client = new WhoopClient({
	clientId: config.clientId,
	clientSecret: config.clientSecret,
	redirectUri: config.redirectUri,
	onTokenRefresh: tokens => db.saveTokens(tokens),
});

const existingTokens = db.getTokens();
if (existingTokens) {
	client.setTokens(existingTokens);
}

const sync = new WhoopSync(client, db);

const SESSION_TTL_MS = 24 * 60 * 60 * 1000;
const transports = new Map<string, { transport: StreamableHTTPServerTransport; lastAccess: number }>();

function cleanupStaleSessions(): void {
	const now = Date.now();
	for (const [sessionId, session] of transports) {
		if (now - session.lastAccess > SESSION_TTL_MS) {
			session.transport.close().catch(() => {});
			transports.delete(sessionId);
		}
	}
}

setInterval(cleanupStaleSessions, 5 * 60 * 1000);

function formatDuration(millis: number | null): string {
	if (millis === null || millis === undefined) return 'N/A';
	const hours = Math.floor(millis / 3_600_000);
	const minutes = Math.floor((millis % 3_600_000) / 60_000);
	return `${hours}h ${minutes}m`;
}

function formatDate(isoString: string): string {
	return new Date(isoString).toLocaleDateString('en-US', {
		weekday: 'short',
		month: 'short',
		day: 'numeric',
		timeZone: config.timezone,
	});
}

// Local date and time of an event, using the offset WHOOP recorded with it (e.g. "+02:00"),
// falling back to the configured time zone.
function formatLocalDateTime(isoString: string, offset: string | null): string {
	const match = offset?.match(/^([+-])(\d{2}):?(\d{2})$/);
	if (!match) {
		return new Date(isoString).toLocaleString('en-US', {
			weekday: 'short', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false,
			timeZone: config.timezone,
		});
	}
	const sign = match[1] === '-' ? -1 : 1;
	const offsetMs = sign * (Number(match[2]) * 60 + Number(match[3])) * 60_000;
	return new Date(new Date(isoString).getTime() + offsetMs).toLocaleString('en-US', {
		weekday: 'short', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false,
		timeZone: 'UTC',
	}) + ` (UTC${offset})`;
}

function escapeHtml(text: string): string {
	return text.replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[ch]!);
}

function safeEqual(a: string, b: string): boolean {
	const bufA = Buffer.from(a);
	const bufB = Buffer.from(b);
	return bufA.length === bufB.length && timingSafeEqual(bufA, bufB);
}

function getRecoveryZone(score: number): string {
	if (score >= 67) return 'Green (Well Recovered)';
	if (score >= 34) return 'Yellow (Moderate)';
	return 'Red (Needs Rest)';
}

function getStrainZone(strain: number): string {
	if (strain >= 18) return 'All Out (18-21)';
	if (strain >= 14) return 'High (14-17)';
	if (strain >= 10) return 'Moderate (10-13)';
	return 'Light (0-9)';
}

function validateDays(value: unknown): number {
	if (value === undefined || value === null) return 14;
	const num = typeof value === 'number' ? value : Number.parseInt(String(value), 10);
	if (Number.isNaN(num) || num < 1) return 14;
	return Math.min(num, 90);
}

function validateBoolean(value: unknown): boolean {
	if (typeof value === 'boolean') return value;
	if (value === 'true') return true;
	return false;
}

function createMcpServer(): Server {
	const server = new Server(
		{ name: 'whoop-mcp-server', version: '1.1.0' },
		{ capabilities: { tools: {} } }
	);

	server.setRequestHandler(ListToolsRequestSchema, async () => ({
		tools: [
			{
				name: 'get_today',
				description: "Get today's Whoop data including recovery score, last night's sleep, and current strain.",
				inputSchema: { type: 'object', properties: {}, required: [] },
			},
			{
				name: 'get_recovery_trends',
				description: 'Get recovery score trends over time, including HRV and resting heart rate patterns.',
				inputSchema: {
					type: 'object',
					properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 90)' } },
					required: [],
				},
			},
			{
				name: 'get_sleep_analysis',
				description: 'Get sleep analysis per night: time asleep, deep and REM sleep, performance and efficiency.',
				inputSchema: {
					type: 'object',
					properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 90)' } },
					required: [],
				},
			},
			{
				name: 'get_strain_history',
				description: 'Get daily strain and calories history (use get_workouts for individual workouts).',
				inputSchema: {
					type: 'object',
					properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 90)' } },
					required: [],
				},
			},
			{
				name: 'get_workouts',
				description: 'Get workout history: sport, date, duration, strain, calories, heart rate, distance and time in each heart rate zone.',
				inputSchema: {
					type: 'object',
					properties: { days: { type: 'number', description: 'Number of days to analyze (default: 14, max: 90)' } },
					required: [],
				},
			},
			{
				name: 'get_profile',
				description: 'Get the WHOOP member profile (name, email, user ID).',
				inputSchema: { type: 'object', properties: {}, required: [] },
			},
			{
				name: 'get_body_measurement',
				description: 'Get body measurements recorded in WHOOP: height, weight and max heart rate.',
				inputSchema: { type: 'object', properties: {}, required: [] },
			},
			{
				name: 'sync_data',
				description: 'Manually trigger a data sync from Whoop.',
				inputSchema: {
					type: 'object',
					properties: { full: { type: 'boolean', description: 'Force a full 90-day sync (default: false)' } },
					required: [],
				},
			},
			{
				name: 'get_auth_url',
				description: 'Get the Whoop authorization URL to connect your account.',
				inputSchema: { type: 'object', properties: {}, required: [] },
			},
		],
	}));

	server.setRequestHandler(CallToolRequestSchema, async request => {
		const { name, arguments: args } = request.params;
		const typedArgs = (args ?? {}) as ToolArguments;

		let syncWarning = '';
		try {
			const dataTools = ['get_today', 'get_recovery_trends', 'get_sleep_analysis', 'get_strain_history', 'get_workouts'];
			if (dataTools.includes(name)) {
				const tokens = db.getTokens();
				if (!tokens) {
					return { content: [{ type: 'text', text: 'Not authenticated with Whoop. Use get_auth_url to authorize first.' }] };
				}
				client.setTokens(tokens);
				try {
					await sync.smartSync();
				} catch (error) {
					// Continue with cached data, but say so: a silent failure hides an expired authorization.
					const message = error instanceof Error ? error.message : 'Unknown error';
					syncWarning = `\n\n> Warning: could not refresh data from WHOOP (${message}). Showing the last synced data. If this persists, run get_auth_url to re-authorize.`;
				}
			}

			switch (name) {
				case 'get_today': {
					const recovery = db.getLatestRecovery();
					const sleep = db.getLatestSleep();
					const cycle = db.getLatestCycle();

					if (!recovery && !sleep && !cycle) {
						return { content: [{ type: 'text', text: 'No data available. Try running sync_data first.' + syncWarning }] };
					}

					let response = "# Today's Whoop Summary\n\n";

					if (recovery) {
						response += `## Recovery (${formatDate(recovery.created_at)}): ${recovery.recovery_score ?? 'N/A'}% ${recovery.recovery_score !== null ? getRecoveryZone(recovery.recovery_score) : `(${recovery.score_state})`}\n`;
						response += `- **HRV**: ${recovery.hrv_rmssd?.toFixed(1) ?? 'N/A'} ms\n`;
						response += `- **Resting HR**: ${recovery.resting_hr ?? 'N/A'} bpm\n`;
						if (recovery.spo2) response += `- **SpO2**: ${recovery.spo2.toFixed(1)}%\n`;
						if (recovery.skin_temp) response += `- **Skin Temp**: ${recovery.skin_temp.toFixed(1)}°C\n`;
						response += '\n';
					}

					if (sleep) {
						const asleep = (sleep.total_light_milli ?? 0) + (sleep.total_deep_milli ?? 0) + (sleep.total_rem_milli ?? 0);
						response += `## Last Night's Sleep (${formatLocalDateTime(sleep.start_time, sleep.timezone_offset)} to ${formatLocalDateTime(sleep.end_time, sleep.timezone_offset)})\n`;
						response += `- **Time Asleep**: ${formatDuration(asleep)}\n`;
						response += `- **Time in Bed**: ${formatDuration(sleep.total_in_bed_milli)}\n`;
						response += `- **Performance**: ${sleep.sleep_performance?.toFixed(0) ?? 'N/A'}%\n`;
						response += `- **Efficiency**: ${sleep.sleep_efficiency?.toFixed(0) ?? 'N/A'}%\n`;
						if (sleep.sleep_consistency !== null) response += `- **Consistency**: ${sleep.sleep_consistency.toFixed(0)}%\n`;
						response += `- **Stages**: Light ${formatDuration(sleep.total_light_milli)}, Deep ${formatDuration(sleep.total_deep_milli)}, REM ${formatDuration(sleep.total_rem_milli)}, Awake ${formatDuration(sleep.total_awake_milli)}\n`;
						if (sleep.respiratory_rate) response += `- **Respiratory Rate**: ${sleep.respiratory_rate.toFixed(1)} breaths/min\n`;
						const need = (sleep.sleep_needed_baseline_milli ?? 0) + (sleep.sleep_needed_debt_milli ?? 0) + (sleep.sleep_needed_strain_milli ?? 0);
						if (need > 0) response += `- **Sleep Needed**: ${formatDuration(need)} (of which sleep debt ${formatDuration(sleep.sleep_needed_debt_milli)})\n`;
						response += '\n';
					}

					if (cycle) {
						response += `## Current Strain (since ${formatLocalDateTime(cycle.start_time, cycle.timezone_offset)})\n`;
						response += `- **Day Strain**: ${cycle.strain?.toFixed(1) ?? 'N/A'} ${cycle.strain !== null ? getStrainZone(cycle.strain) : ''}\n`;
						if (cycle.kilojoule) response += `- **Calories**: ${Math.round(cycle.kilojoule / 4.184)} kcal\n`;
						if (cycle.avg_hr) response += `- **Avg HR**: ${cycle.avg_hr} bpm\n`;
						if (cycle.max_hr) response += `- **Max HR**: ${cycle.max_hr} bpm\n`;
					}

					return { content: [{ type: 'text', text: response + syncWarning }] };
				}

				case 'get_recovery_trends': {
					const days = validateDays(typedArgs.days);
					const trends = db.getRecoveryTrends(days);

					if (trends.length === 0) {
						return { content: [{ type: 'text', text: 'No recovery data available for the requested period.' }] };
					}

					let response = `# Recovery Trends (Last ${days} Days)\n\n`;
					response += '| Date | Recovery | HRV | RHR |\n|------|----------|-----|-----|\n';

					for (const day of trends) {
						response += `| ${formatDate(day.date)} | ${day.recovery_score}% | ${day.hrv?.toFixed(1) ?? 'N/A'} ms | ${day.rhr ?? 'N/A'} bpm |\n`;
					}

					const avgRecovery = trends.reduce((sum, d) => sum + (d.recovery_score || 0), 0) / trends.length;
					const avgHrv = trends.reduce((sum, d) => sum + (d.hrv || 0), 0) / trends.length;
					const avgRhr = trends.reduce((sum, d) => sum + (d.rhr || 0), 0) / trends.length;

					response += `\n## Averages\n- **Recovery**: ${avgRecovery.toFixed(0)}%\n- **HRV**: ${avgHrv.toFixed(1)} ms\n- **RHR**: ${avgRhr.toFixed(0)} bpm\n`;

					return { content: [{ type: 'text', text: response + syncWarning }] };
				}

				case 'get_sleep_analysis': {
					const days = validateDays(typedArgs.days);
					const trends = db.getSleepTrends(days);

					if (trends.length === 0) {
						return { content: [{ type: 'text', text: 'No sleep data available for the requested period.' }] };
					}

					let response = `# Sleep Analysis (Last ${days} Days)\n\n`;
					response += '| Night ending | Asleep | Deep | REM | Performance | Efficiency |\n|------|--------|------|-----|-------------|------------|\n';

					for (const day of trends) {
						response += `| ${formatDate(day.date)} | ${day.total_sleep_hours?.toFixed(1) ?? 'N/A'}h | ${day.deep_hours?.toFixed(1) ?? 'N/A'}h | ${day.rem_hours?.toFixed(1) ?? 'N/A'}h | ${day.performance?.toFixed(0) ?? 'N/A'}% | ${day.efficiency?.toFixed(0) ?? 'N/A'}% |\n`;
					}

					const avgDuration = trends.reduce((sum, d) => sum + (d.total_sleep_hours || 0), 0) / trends.length;
					const avgPerf = trends.reduce((sum, d) => sum + (d.performance || 0), 0) / trends.length;
					const avgEff = trends.reduce((sum, d) => sum + (d.efficiency || 0), 0) / trends.length;

					response += `\n## Averages\n- **Time Asleep**: ${avgDuration.toFixed(1)} hours\n- **Performance**: ${avgPerf.toFixed(0)}%\n- **Efficiency**: ${avgEff.toFixed(0)}%\n`;

					return { content: [{ type: 'text', text: response + syncWarning }] };
				}

				case 'get_strain_history': {
					const days = validateDays(typedArgs.days);
					const trends = db.getStrainTrends(days);

					if (trends.length === 0) {
						return { content: [{ type: 'text', text: 'No strain data available for the requested period.' }] };
					}

					let response = `# Strain History (Last ${days} Days)\n\n`;
					response += '| Date | Strain | Calories |\n|------|--------|----------|\n';

					for (const day of trends) {
						response += `| ${formatDate(day.date)} | ${day.strain?.toFixed(1) ?? 'N/A'} | ${day.calories ?? 'N/A'} kcal |\n`;
					}

					const avgStrain = trends.reduce((sum, d) => sum + (d.strain || 0), 0) / trends.length;
					const avgCalories = trends.reduce((sum, d) => sum + (d.calories || 0), 0) / trends.length;

					response += `\n## Averages\n- **Daily Strain**: ${avgStrain.toFixed(1)}\n- **Daily Calories**: ${Math.round(avgCalories)} kcal\n`;

					return { content: [{ type: 'text', text: response + syncWarning }] };
				}

				case 'get_workouts': {
					const days = validateDays(typedArgs.days);
					const workouts = db.getWorkouts(days);

					if (workouts.length === 0) {
						return { content: [{ type: 'text', text: 'No workouts recorded for the requested period.' }] };
					}

					const zoneLabels = ['Z0', 'Z1', 'Z2', 'Z3', 'Z4', 'Z5'];
					let response = `# Workouts (Last ${days} Days): ${workouts.length} sessions\n\n`;

					for (const w of workouts) {
						const durationMs = new Date(w.end_time).getTime() - new Date(w.start_time).getTime();
						const sport = w.sport_name ?? `sport ${w.sport_id}`;
						response += `## ${formatDate(w.start_time)}: ${sport}\n`;
						response += `- **Start**: ${formatLocalDateTime(w.start_time, w.timezone_offset)}\n`;
						response += `- **Duration**: ${formatDuration(durationMs)}\n`;
						if (w.score_state !== 'SCORED') response += `- **Score state**: ${w.score_state}\n`;
						if (w.strain !== null) response += `- **Strain**: ${w.strain.toFixed(1)}\n`;
						if (w.kilojoule !== null) response += `- **Calories**: ${Math.round(w.kilojoule / 4.184)} kcal\n`;
						if (w.avg_hr !== null) response += `- **Avg / Max HR**: ${w.avg_hr} / ${w.max_hr ?? 'N/A'} bpm\n`;
						if (w.distance_meter) response += `- **Distance**: ${(w.distance_meter / 1000).toFixed(2)} km\n`;
						if (w.altitude_gain_meter) response += `- **Altitude gain**: ${Math.round(w.altitude_gain_meter)} m\n`;
						if (w.percent_recorded !== null) {
							// WHOOP returns a 0-1 fraction (1 = 100%)
							const pct = w.percent_recorded <= 1 ? w.percent_recorded * 100 : w.percent_recorded;
							response += `- **HR recorded**: ${Math.round(pct)}%\n`;
						}
						const zones = [w.zone_zero_milli, w.zone_one_milli, w.zone_two_milli, w.zone_three_milli, w.zone_four_milli, w.zone_five_milli];
						if (zones.some(z => z !== null)) {
							response += `- **HR zones**: ${zones.map((z, i) => `${zoneLabels[i]} ${formatDuration(z ?? 0)}`).join(', ')}\n`;
						}
						response += '\n';
					}

					return { content: [{ type: 'text', text: response + syncWarning }] };
				}

				case 'sync_data': {
					const tokens = db.getTokens();
					if (!tokens) {
						return { content: [{ type: 'text', text: 'Not authenticated with Whoop. Use get_auth_url to authorize first.' }] };
					}
					client.setTokens(tokens);

					const full = validateBoolean(typedArgs.full);
					let stats;

					if (full) {
						stats = await sync.syncDays(90);
					} else {
						const result = await sync.smartSync();
						if (result.type === 'skip') {
							return { content: [{ type: 'text', text: 'Data is already up to date (synced within the last hour).' }] };
						}
						stats = result.stats;
					}

					return {
						content: [{
							type: 'text',
							text: `Sync complete!\n- Cycles: ${stats?.cycles}\n- Recoveries: ${stats?.recoveries}\n- Sleeps: ${stats?.sleeps}\n- Workouts: ${stats?.workouts}`,
						}],
					};
				}

				case 'get_profile':
				case 'get_body_measurement': {
					const tokens = db.getTokens();
					if (!tokens) {
						return { content: [{ type: 'text', text: 'Not authenticated with Whoop. Use get_auth_url to authorize first.' }] };
					}
					client.setTokens(tokens);

					if (name === 'get_profile') {
						const p = await client.getProfile();
						return {
							content: [{
								type: 'text',
								text: `# WHOOP Profile\n- **Name**: ${p.first_name} ${p.last_name}\n- **Email**: ${p.email}\n- **User ID**: ${p.user_id}\n`,
							}],
						};
					}

					const b = await client.getBodyMeasurement();
					return {
						content: [{
							type: 'text',
							text: `# Body Measurements\n- **Height**: ${(b.height_meter * 100).toFixed(0)} cm\n- **Weight**: ${b.weight_kilogram.toFixed(1)} kg\n- **Max Heart Rate**: ${b.max_heart_rate} bpm\n`,
						}],
					};
				}

				case 'get_auth_url': {
					const state = randomUUID().replaceAll('-', '');
					db.saveOAuthState(state);
					const url = client.getAuthorizationUrl(OAUTH_SCOPES, state);
					return {
						content: [{
							type: 'text',
							text: `To authorize with Whoop:\n\n1. Visit: ${url}\n2. Log in and authorize\n3. You'll be redirected back automatically\n\nThis link is valid for 30 minutes and can be used once.\nRedirect URI: ${config.redirectUri}`,
						}],
					};
				}

				default:
					throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${name}`);
			}
		} catch (error) {
			const message = error instanceof Error ? error.message : 'Unknown error';
			return { content: [{ type: 'text', text: `Error: ${message}${syncWarning}` }], isError: true };
		}
	});

	return server;
}

async function main(): Promise<void> {
	if (config.mode === 'stdio') {
		const server = createMcpServer();
		const transport = new StdioServerTransport();
		await server.connect(transport);
		process.stderr.write('Whoop MCP server running on stdio\n');
	} else {
		const app = express();
		app.use(express.json());

		app.get('/callback', async (req: Request, res: Response) => {
			const page = (title: string, detail = ''): string =>
				`<!doctype html><meta charset="utf-8"><title>WHOOP</title><body style="font-family:system-ui;max-width:560px;margin:15vh auto;padding:0 16px"><h2>${escapeHtml(title)}</h2><p>${escapeHtml(detail)}</p></body>`;

			const error = typeof req.query.error === 'string' ? req.query.error : undefined;
			if (error) {
				const description = typeof req.query.error_description === 'string' ? req.query.error_description : '';
				process.stdout.write(`${JSON.stringify({ event: 'oauth_callback_error', error, description })}\n`);
				res.status(400).send(page(`WHOOP refused the authorization: ${error}`, description));
				return;
			}

			const code = typeof req.query.code === 'string' ? req.query.code : undefined;
			const state = typeof req.query.state === 'string' ? req.query.state : undefined;
			if (!code) {
				res.status(400).send(page('Missing authorization code'));
				return;
			}
			if (!state || !db.consumeOAuthState(state)) {
				res.status(400).send(page('Invalid or expired authorization link', 'Ask Claude for a new link with get_auth_url and try again.'));
				return;
			}

			try {
				const tokens = await client.exchangeCodeForTokens(code);
				db.saveTokens(tokens);
				sync.syncDays(90).catch(err => {
					process.stdout.write(`${JSON.stringify({ event: 'initial_sync_error', message: String(err) })}\n`);
				});
				res.send(page('Authorization successful!', 'You can close this window.'));
			} catch (err) {
				process.stdout.write(`${JSON.stringify({ event: 'token_exchange_error', message: String(err) })}\n`);
				res.status(500).send(page('Authorization failed', 'Please ask Claude for a new link and try again.'));
			}
		});

		app.get('/health', (_req: Request, res: Response) => {
			res.json({ status: 'ok', authenticated: Boolean(db.getTokens()) });
		});

		if (!config.accessKey) {
			process.stdout.write('Warning: MCP_ACCESS_KEY is not set, the /mcp endpoint is open to anyone who knows the URL.\n');
		}

		app.all(['/mcp', '/mcp/:key'], async (req: Request, res: Response) => {
			// Access control: with MCP_ACCESS_KEY set, only /mcp/<key> is accepted.
			const providedKey = req.params.key ?? '';
			if (config.accessKey ? !safeEqual(providedKey, config.accessKey) : providedKey !== '') {
				res.status(404).send('Not found');
				return;
			}

			const sessionId = req.headers['mcp-session-id'] as string | undefined;

			if (req.method === 'DELETE') {
				if (sessionId && transports.has(sessionId)) {
					const session = transports.get(sessionId)!;
					await session.transport.close();
					transports.delete(sessionId);
					res.status(200).send('Session closed');
				} else {
					res.status(404).send('Session not found');
				}
				return;
			}

			if (req.method === 'POST') {
				let transport: StreamableHTTPServerTransport;

				if (sessionId && transports.has(sessionId)) {
					const session = transports.get(sessionId)!;
					session.lastAccess = Date.now();
					transport = session.transport;
				} else {
					const body = req.body as { method?: string } | Array<{ method?: string }> | undefined;
					const isInitialize = Array.isArray(body)
						? body.some(msg => msg?.method === 'initialize')
						: body?.method === 'initialize';

					// Unknown or expired session (e.g. after a redeploy): answer 404 so the
					// client starts a fresh session instead of failing with "Server not initialized".
					if (!isInitialize) {
						res.status(404).json({
							jsonrpc: '2.0',
							error: { code: -32001, message: 'Session not found, please reinitialize' },
							id: null,
						});
						return;
					}

					transport = new StreamableHTTPServerTransport({
						sessionIdGenerator: () => randomUUID(),
						onsessioninitialized: newSessionId => {
							transports.set(newSessionId, { transport, lastAccess: Date.now() });
						},
					});
					transport.onclose = () => {
						if (transport.sessionId) transports.delete(transport.sessionId);
					};

					const server = createMcpServer();
					await server.connect(transport);
				}

				await transport.handleRequest(req, res, req.body);
				return;
			}

			res.status(405).send('Method not allowed');
		});

		app.get('/sse', (_req: Request, res: Response) => {
			res.status(410).send('SSE endpoint deprecated. Use /mcp with Streamable HTTP transport.');
		});

		const server = app.listen(config.port, '0.0.0.0', () => {
			process.stdout.write(`Whoop MCP server running on http://0.0.0.0:${config.port}\n`);
		});

		const shutdown = (): void => {
			process.stdout.write('\nShutting down...\n');
			for (const [, session] of transports) {
				session.transport.close().catch(() => {});
			}
			transports.clear();
			db.close();
			server.close(() => process.exit(0));
		};

		process.on('SIGTERM', shutdown);
		process.on('SIGINT', shutdown);
	}
}

main().catch(error => {
	process.stderr.write(`Fatal error: ${error}\n`);
	process.exit(1);
});
