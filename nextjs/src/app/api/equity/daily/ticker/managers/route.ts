import { NextResponse } from "next/server";

export async function GET(req: Request) {
         const url = new URL(req.url);

  // forward ALL query params (date, scenario, ret_shift, etc.)
  const queryString = url.searchParams.toString();
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;

  if (!baseUrl) {
    throw new Error("NEXT_PUBLIC_API_BASE_URL is not defined");
  }

  const res = await fetch(
    `${baseUrl}/api/daily/equity/ticker/managers/?${queryString}`,
    { cache: "no-store" }
  );
console.log(queryString)
  return NextResponse.json(await res.json());
}
