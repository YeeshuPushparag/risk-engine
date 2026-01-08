import { NextResponse } from "next/server";

export async function GET() {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!baseUrl) throw new Error("API base URL not defined");

  const res = await fetch(
    `${baseUrl}/api/daily/bonds/overview/`,
    { cache: "no-store" }
  );

  const data = await res.json();
  return NextResponse.json(data);
}
