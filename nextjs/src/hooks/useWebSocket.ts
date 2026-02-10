"use client";

import { useEffect, useRef } from "react";

export function useWebSocket(
  url: string | null,
  onMessage: (data: any) => void
) {
  const wsRef = useRef<WebSocket | null>(null);
  const callbackRef = useRef(onMessage);
  const connectionId = useRef(Date.now());

  useEffect(() => {
    callbackRef.current = onMessage;
  }, [onMessage]);

  useEffect(() => {
    console.log(`WebSocket hook: url = ${url ? url : 'NULL'}`);
    
    if (!url) {
      console.log("No URL, closing existing connection");
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      return;
    }

    if (wsRef.current) {
      console.log("Closing previous WebSocket");
      wsRef.current.close();
    }

    console.log(`Creating WebSocket in 100ms to: ${url}`);
    
    const timer = setTimeout(() => {
      console.log(`Creating WebSocket to: ${url}`);
      const ws = new WebSocket(url);
      wsRef.current = ws;
      connectionId.current = Date.now();

      ws.onopen = () => {
        console.log(`WebSocket CONNECTED to: ${url}`);
      };

      ws.onmessage = (event) => {
        console.log(`WebSocket message received`);
        try {
          const data = JSON.parse(event.data);
          callbackRef.current(data);
        } catch (err) {
          console.warn("WebSocket parse error");
        }
      };

      ws.onerror = (err) => {
        console.error("WebSocket error:", err);
      };

      ws.onclose = (event) => {
        console.log(`WebSocket closed, code: ${event.code}`);
        wsRef.current = null;
      };
    }, 100);

    return () => {
      console.log("WebSocket cleanup");
      clearTimeout(timer);
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [url]);
}