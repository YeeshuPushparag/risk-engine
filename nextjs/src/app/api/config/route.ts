// app/api/config/route.ts
import { NextResponse } from "next/server";

export async function GET() {
  // Read from server env — this is available at runtime
  const wsBaseUrl = process.env.WS_BASE_URL || "ws://localhost:8000/ws";
  const forceStream = process.env.FORCE_STREAM === "true";

  return NextResponse.json({
    wsBaseUrl,
    forceStream,
  });
}
