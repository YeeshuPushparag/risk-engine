import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const base = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!base) throw new Error("API base not set");

  const { searchParams } = new URL(req.url);
  const ticker = searchParams.get("ticker");

  if (!ticker) {
    return NextResponse.json({ error: "Missing ticker" }, { status: 400 });
  }

  const res = await fetch(
    `${base}/api/daily/loans/ticker/?ticker=${ticker}`,
    { cache: "no-store" }
  );

  const data = await res.json();
  return NextResponse.json(data);
}
