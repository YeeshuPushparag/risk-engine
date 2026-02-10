import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const ticker = searchParams.get("ticker");
  const asset_class = searchParams.get("asset_class");

  if (!ticker || !asset_class) {
    return NextResponse.json(
      { error: "Missing ticker or asset_class" },
      { status: 400 }
    );
  }

  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  const apiUrl = `${baseUrl}/api/daily/derivatives/ticker-asset/?ticker=${ticker}&asset_class=${asset_class}`;

  const res = await fetch(apiUrl, { cache: "no-store" });
  const data = await res.json();

  return NextResponse.json(data);
}
