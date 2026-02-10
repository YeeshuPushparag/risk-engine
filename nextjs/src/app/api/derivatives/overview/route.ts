import { NextResponse } from "next/server";

export async function GET() {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const apiUrl = `${baseUrl}/api/daily/derivatives/overview/`;

  const res = await fetch(apiUrl, { cache: "no-store" });
  const data = await res.json();

  return NextResponse.json(data);
}
