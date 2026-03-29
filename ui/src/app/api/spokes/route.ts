import { NextResponse } from "next/server";
import { listSpokes } from "@/lib/hub-client";

/**
 * GET /api/spokes
 *
 * Proxies to the Hub's ListSpokes gRPC call.  Returns all registered spokes
 * with health state, active captures, last heartbeat, etc.
 */
export async function GET() {
  const spokes = await listSpokes();
  return NextResponse.json({ spokes });
}
