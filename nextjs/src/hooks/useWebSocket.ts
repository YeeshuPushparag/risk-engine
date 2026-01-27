"use client";

import { useEffect, useRef } from "react";

export function useWebSocket(
  url: string | null,
  onMessage: (data: any) => void
) {
  const callbackRef = useRef(onMessage);

  // Always keep latest callback
  
  useEffect(() => {
    callbackRef.current = onMessage;
  }, [onMessage]);

  useEffect(() => {
    if (!url) return;

    const ws = new WebSocket(url);

    ws.onopen = () => {
      console.log("WS connected:", url);
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        callbackRef.current(data);
      } catch (err) {
        console.warn("WS parse error:", event.data);
      }
    };

    ws.onerror = (err) => {
      console.error("WS error:", err);
    };

    ws.onclose = () => {
      console.log("WS closed:", url);
    };

    return () => {
      ws.close();
    };
  }, [url]);
}
