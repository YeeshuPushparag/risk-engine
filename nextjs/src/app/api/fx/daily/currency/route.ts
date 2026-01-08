import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!baseUrl) {
    throw new Error("API base URL not defined");
  }

  const { searchParams } = new URL(req.url);
  const currency = searchParams.get("currency");

  if (!currency) {
    return NextResponse.json(
      { error: "Missing currency parameter" },
      { status: 400 }
    );
  }

  const apiUrl = `${baseUrl}/api/daily/fx/currency/?currency=${currency}`;

  const res = await fetch(apiUrl, { cache: "no-store" });

  if (!res.ok) {
    return NextResponse.json(
      { error: "Failed to fetch FX currency detail" },
      { status: res.status }
    );
  }

  const data = await res.json();
  return NextResponse.json(data);
}
