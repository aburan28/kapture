import { NextRequest, NextResponse } from "next/server";
import { getCaptureStatus, getCapturedRequests } from "@/lib/hub-client";

/**
 * GET /api/captures/:name?namespace=...
 *
 * Proxies to the Hub's GetCaptureStatus gRPC call.  Returns the full
 * TrafficCapture detail including recent captured requests.
 */
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ name: string }> }
) {
  const { name } = await params;
  const namespace =
    request.nextUrl.searchParams.get("namespace") ?? "kapture-system";

  const capture = await getCaptureStatus(name, namespace);
  if (!capture) {
    return NextResponse.json(
      { error: "Capture not found" },
      { status: 404 }
    );
  }

  const requests = await getCapturedRequests(name);
  return NextResponse.json({ capture, recentRequests: requests });
}
