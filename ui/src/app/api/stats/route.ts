import { NextRequest, NextResponse } from "next/server";
import { getCaptureStats } from "@/lib/hub-client";

/**
 * GET /api/stats?capture=namespace/name
 *
 * The capture's streaming-statistics snapshot (flow windows,
 * cardinality, heavy hitters, quantiles) as served by the agent's
 * /stats endpoint. Mock data when no agent is configured.
 */
export async function GET(req: NextRequest) {
  const capture = req.nextUrl.searchParams.get("capture") ?? "";
  const { snapshot, mock } = await getCaptureStats(capture);
  return NextResponse.json({ capture, mock, snapshot });
}
