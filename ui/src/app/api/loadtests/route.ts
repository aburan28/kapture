import { NextResponse } from "next/server";
import { listLoadTests } from "@/lib/hub-client";

/**
 * GET /api/loadtests
 *
 * Lists CaptureLoadTest resources with their distribution and aggregated
 * shard progress.
 */
export async function GET() {
  const loadTests = await listLoadTests();
  return NextResponse.json({ loadTests });
}
