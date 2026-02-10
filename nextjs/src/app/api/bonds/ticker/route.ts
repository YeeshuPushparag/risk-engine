import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!baseUrl) throw new Error("API base URL not defined");

  const { searchParams } = new URL(req.url);
  const ticker = searchParams.get("ticker");

  if (!ticker) {
    return NextResponse.json(
      { error: "Missing ticker" },
      { status: 400 }
    );
  }

  const res = await fetch(
    `${baseUrl}/api/daily/bonds/ticker/?ticker=${ticker}`,
    { cache: "no-store" }
  );

  const data = await res.json();
  return NextResponse.json(data);
}
