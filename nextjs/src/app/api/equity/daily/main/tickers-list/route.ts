import { NextResponse } from "next/server";

export async function GET(request: Request) {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;

  if (!baseUrl) {
    throw new Error("NEXT_PUBLIC_API_BASE_URL is not defined");
  }

  const { searchParams } = new URL(request.url);
  const type = searchParams.get("type") || "daily";

  const res = await fetch(
    `${baseUrl}/api/daily/equity/tickers-list/?type=${type}`,
    { cache: "no-store" }
  );

  const data = await res.json();
  return NextResponse.json(data);
}
