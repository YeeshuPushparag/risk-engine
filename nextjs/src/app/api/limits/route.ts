import { NextResponse } from "next/server";

export async function GET(req: Request) {
  try {
    // 1. Extract search params from the incoming URL (e.g., ?archive=true)
    const { searchParams } = new URL(req.url);
    const archive = searchParams.get("archive") || "false";

    // 2. Forward the 'archive' variable to your Django Risk Engine
    const apiUrl = `${process.env.NEXT_PUBLIC_API_BASE_URL}/api/daily/risk/compliance/limits/?archive=${archive}`;

    const res = await fetch(apiUrl, { cache: 'no-store' });
    
    if (!res.ok) {
        throw new Error(`Risk Engine responded with status: ${res.status}`);
    }

    const data = await res.json();
    return NextResponse.json(data);

  } catch (e) {
    console.error("Route Handler Error:", e);
    return NextResponse.json(
      { error: "Failed to connect to Risk Engine" }, 
      { status: 500 }
    );
  }
}