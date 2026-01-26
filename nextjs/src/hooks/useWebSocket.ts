"use client";

import { useEffect, useRef } from "react";

export function useWebSocket(
  url: string | null,
  enabled: boolean,             // <-- add enabled flag
  onMessage: (data: any) => void
) {
  const callbackRef = useRef(onMessage);

  // keep ref updated so latest callback is used
  useEffect(() => {
    callbackRef.current = onMessage;
  }, [onMessage]);

  useEffect(() => {
    if (!url || !enabled) return; // wait until both are set

    const ws = new WebSocket(url);

    ws.onopen = () => console.log("WS connected:", url);

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        callbackRef.current(data);
      } catch {
        console.warn("WS parse error", event.data);
      }
    };

    ws.onerror = (err) => console.error("WS error:", err);

    ws.onclose = () => console.log("WS closed:", url);

    return () => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    };
  }, [url, enabled]); // <-- watch both url and enabled
}
