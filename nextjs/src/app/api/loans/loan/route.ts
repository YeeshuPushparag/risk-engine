import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const base = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!base) throw new Error("API base not set");

  const { searchParams } = new URL(req.url);
  const loan_id = searchParams.get("loan_id");

  if (!loan_id) {
    return NextResponse.json({ error: "Missing loan_id" }, { status: 400 });
  }

  const res = await fetch(
    `${base}/api/daily/loans/loan/?loan_id=${loan_id}`,
    { cache: "no-store" }
  );

  const data = await res.json();
  return NextResponse.json(data);
}
