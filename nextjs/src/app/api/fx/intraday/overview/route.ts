import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const queryString = url.searchParams.toString();

  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!baseUrl) throw new Error("API base URL undefined");

  const apiUrl = `${baseUrl}/api/intraday/fx/overview/?${queryString}`;

  const res = await fetch(apiUrl, { cache: "no-store" });
  const data = await res.json();

  return NextResponse.json(data);
}
