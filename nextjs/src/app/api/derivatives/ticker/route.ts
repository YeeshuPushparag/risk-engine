import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const ticker = url.searchParams.get("ticker");

  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const apiUrl = `${baseUrl}/api/daily/derivatives/ticker/?ticker=${ticker}`;

  const res = await fetch(apiUrl, { cache: "no-store" });
  const data = await res.json();

  return NextResponse.json(data);
}
