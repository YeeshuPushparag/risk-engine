import { NextResponse } from "next/server";

export async function GET() {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;

  if (!baseUrl) {
    throw new Error("NEXT_PUBLIC_API_BASE_URL is not defined");
  }

  const res = await fetch(
    `${baseUrl}/api/intraday/equity/tickers-list/`,
    { cache: "no-store" }
  );

  const data = await res.json();
  return NextResponse.json(data);
}
