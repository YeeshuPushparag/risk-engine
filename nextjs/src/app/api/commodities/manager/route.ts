import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const manager = searchParams.get("manager");

  const base = process.env.NEXT_PUBLIC_API_BASE_URL!;
  const res = await fetch(
    `${base}/api/daily/commodities/manager/?manager=${manager}`,
    { cache: "no-store" }
  );

  return NextResponse.json(await res.json());
}
