import { NextResponse } from "next/server";

export async function GET() {
  const base = process.env.NEXT_PUBLIC_API_BASE_URL;
  const res = await fetch(`${base}/api/daily/collateral/overview/`, {
    cache: "no-store",
  });

  return NextResponse.json(await res.json());
}
