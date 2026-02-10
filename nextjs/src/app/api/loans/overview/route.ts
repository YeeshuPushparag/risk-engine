import { NextResponse } from "next/server";

export async function GET() {
  const base = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!base) throw new Error("API base not set");

  const res = await fetch(`${base}/api/daily/loans/overview/`, {
    cache: "no-store",
  });

  const data = await res.json();
  return NextResponse.json(data);
}
