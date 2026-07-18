import { NextResponse } from "next/server";
import { listCells } from "@/lib/hub-client";

/**
 * GET /api/cells
 *
 * Cell topology: spokes grouped by their registered cell.
 */
export async function GET() {
  const cells = await listCells();
  return NextResponse.json({ cells });
}
