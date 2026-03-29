import { NextRequest, NextResponse } from "next/server";
import { listCaptures } from "@/lib/hub-client";

/**
 * GET /api/captures?spoke_id=...&namespace=...
 *
 * Proxies to the Hub's ListCaptures gRPC call.  Returns the list of
 * TrafficCaptures across all connected spokes (optionally filtered).
 */
export async function GET(request: NextRequest) {
  const spokeId = request.nextUrl.searchParams.get("spoke_id") ?? undefined;
  const namespace = request.nextUrl.searchParams.get("namespace") ?? undefined;

  const captures = await listCaptures({ spokeId, namespace });
  return NextResponse.json({ captures });
}
