import { NextRequest, NextResponse } from "next/server";
import { getLoadTest, listReplays } from "@/lib/hub-client";

/**
 * GET /api/loadtests/:name
 *
 * A single CaptureLoadTest plus its replay shards.
 */
export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ name: string }> }
) {
  const { name } = await params;
  const namespace = req.nextUrl.searchParams.get("namespace") ?? undefined;

  const loadTest = await getLoadTest(name, namespace);
  if (!loadTest) {
    return NextResponse.json({ error: "not found" }, { status: 404 });
  }
  const shards = await listReplays({ loadTest: name });
  return NextResponse.json({ loadTest, shards });
}
