import { WhoopClient } from './whoop-client.js';
import { WhoopDatabase } from './database.js';

interface SyncStats {
	cycles: number;
	recoveries: number;
	sleeps: number;
	workouts: number;
}

interface SmartSyncResult {
	type: 'full' | 'quick' | 'skip';
	stats?: SyncStats;
}

export class WhoopSync {
	private readonly client: WhoopClient;
	private readonly db: WhoopDatabase;

	constructor(client: WhoopClient, db: WhoopDatabase) {
		this.client = client;
		this.db = db;
	}

	async syncDays(days = 90): Promise<SyncStats> {
		const endDate = new Date();
		const startDate = new Date();
		startDate.setDate(startDate.getDate() - days);

		const start = startDate.toISOString();
		const end = endDate.toISOString();

		const [cycles, recoveries, sleeps, workouts] = await Promise.all([
			this.client.getAllCycles({ start, end }),
			this.client.getAllRecoveries({ start, end }),
			this.client.getAllSleeps({ start, end }),
			this.client.getAllWorkouts({ start, end }),
		]);

		if (cycles.length > 0) this.db.upsertCycles(cycles);
		if (recoveries.length > 0) this.db.upsertRecoveries(recoveries);
		if (sleeps.length > 0) this.db.upsertSleeps(sleeps);
		if (workouts.length > 0) this.db.upsertWorkouts(workouts);

		this.db.updateSyncState(
			startDate.toISOString().split('T')[0],
			endDate.toISOString().split('T')[0]
		);

		return {
			cycles: cycles.length,
			recoveries: recoveries.length,
			sleeps: sleeps.length,
			workouts: workouts.length,
		};
	}

	// Re-syncs from a couple of days before the newest synced date, so that a long
	// gap without any call (more than a week) does not leave missing days.
	async quickSync(): Promise<SyncStats> {
		const state = this.db.getSyncState();
		let days = 7;
		if (state.newestDate) {
			const sinceNewest = Math.ceil((Date.now() - new Date(state.newestDate).getTime()) / 86_400_000);
			days = Math.max(days, sinceNewest + 2);
		}
		return this.syncDays(Math.min(days, 90));
	}

	needsFullSync(): boolean {
		const state = this.db.getSyncState();
		if (!state.lastSyncAt) return true;

		const lastSync = new Date(state.lastSyncAt);
		const hoursSinceSync = (Date.now() - lastSync.getTime()) / (1000 * 60 * 60);
		return hoursSinceSync > 24;
	}

	async smartSync(): Promise<SmartSyncResult> {
		const state = this.db.getSyncState();

		if (!state.lastSyncAt) {
			const stats = await this.syncDays(90);
			return { type: 'full', stats };
		}

		const lastSync = new Date(state.lastSyncAt);
		const hoursSinceSync = (Date.now() - lastSync.getTime()) / (1000 * 60 * 60);

		if (hoursSinceSync < 1) {
			return { type: 'skip' };
		}

		const stats = await this.quickSync();
		return { type: 'quick', stats };
	}
}
