import { NextRequest, NextResponse } from "next/server";
import { listReplays } from "@/lib/hub-client";

/**
 * GET /api/replays?loadTest=&namespace=
 *
 * Replay shards aggregated by the hub from spoke reports.
 */
export async function GET(req: NextRequest) {
  const loadTest = req.nextUrl.searchParams.get("loadTest") ?? undefined;
  const namespace = req.nextUrl.searchParams.get("namespace") ?? undefined;
  const replays = await listReplays({ loadTest, namespace });
  return NextResponse.json({ replays });
}
