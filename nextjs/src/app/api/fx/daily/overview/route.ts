import { NextResponse } from "next/server";

export async function GET() {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
  if (!baseUrl) {
    throw new Error("API base URL undefined");
  }

  const apiUrl = `${baseUrl}/api/daily/fx/overview/`;

  const res = await fetch(apiUrl, {
    cache: "no-store",
  });

  if (!res.ok) {
    return NextResponse.json(
      { error: "Failed to fetch FX overview" },
      { status: res.status }
    );
  }

  const data = await res.json();
  return NextResponse.json(data);
}
