import { NextResponse } from "next/server";

export async function GET() {
  try {
    const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;
    const res = await fetch(`${baseUrl}/api/daily/risk-dashboard/`, { cache: "no-store" });
    const data = await res.json();

    const formatFinancial = (val: number) => {
      const abs = Math.abs(val);
      if (abs >= 1_000_000_000_000) return `$${(val / 1_000_000_000_000).toFixed(2)}T`;
      if (abs >= 1_000_000_000) return `$${(val / 1_000_000_000).toFixed(2)}B`;
      if (abs >= 1_000_000) return `$${(val / 1_000_000).toFixed(2)}M`;
      return `$${(val / 1_000).toFixed(1)}K`;
    };

    const formattedSegments = data.segments.map((s: any) => ({
      ...s,
      exposure: formatFinancial(s.exposure),
      pnl: `${s.pnl >= 0 ? "+" : ""}${formatFinancial(s.pnl)}`,
      // Logic for color
      riskColor: Number(s.alerts) === 0 ? "green" : s.riskColor,
    }));

    return NextResponse.json({
      total_gross_exposure: formatFinancial(data.total_gross_exposure),
      segments: formattedSegments,
      metadata: data.metadata
    });
  } catch (e) {
    return NextResponse.json({ error: "Connection Error" }, { status: 500 });
  }
}