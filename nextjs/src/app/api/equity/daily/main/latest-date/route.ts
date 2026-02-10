import { NextResponse } from "next/server";

export async function GET() {

  // forward ALL query params (date, scenario, ret_shift, etc.)
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;

  if (!baseUrl) {
    throw new Error("NEXT_PUBLIC_API_BASE_URL is not defined");
  }

  const res = await fetch(
    `${baseUrl}/api/daily/latest-date/`,
    { cache: "no-store" }
  );

  const data = await res.json();
  return NextResponse.json(data);
}
