import { NextResponse } from "next/server";

export async function GET(req: Request) {

  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!baseUrl) throw new Error("Missing NEXT_PUBLIC_API_BASE_URL");

  const url = new URL(req.url);
  const manager = url.searchParams.get("manager") ?? "";

  const res = await fetch(
    `${baseUrl}/api/intraday/equity/manager/?manager=${manager}`,
    { cache: "no-store" }
  );

  const json = await res.json();
  return NextResponse.json(json);
}
