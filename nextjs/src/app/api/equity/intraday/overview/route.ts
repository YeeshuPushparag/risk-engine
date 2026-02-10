import { NextResponse } from "next/server";

export async function GET(req: Request) {

  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!baseUrl) throw new Error("Missing NEXT_PUBLIC_API_BASE_URL");

  const res = await fetch(
    `${baseUrl}/api/intraday/equity/overview/`,
    { cache: "no-store" }
  );

  const json = await res.json();
  return NextResponse.json(json);
}
