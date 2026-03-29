import { NextResponse } from "next/server";
import { listStorages } from "@/lib/hub-client";

/**
 * GET /api/storages
 *
 * Returns configured CaptureStorage backends.  Currently reads from the
 * Kubernetes API (or mock data).  A future hub endpoint may aggregate
 * storage info across spokes.
 */
export async function GET() {
  const storages = await listStorages();
  return NextResponse.json({ storages });
}
