import { createContext, useState, useEffect, useRef } from "react";
import API from "../api/axios";

export const AuthContext = createContext();

// How many ms before expiry to proactively refresh (4 minutes)
const REFRESH_BEFORE_MS = 4 * 60 * 1000;

function parseJwtExpiry(token) {
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    return payload.exp ? payload.exp * 1000 : null;
  } catch {
    return null;
  }
}

export const AuthProvider = ({ children }) => {
  const [user, setUser]       = useState(null);
  const [ready, setReady]     = useState(false); // wait for initial auth check
  const refreshTimer          = useRef(null);

  const scheduleRefresh = (accessToken) => {
    if (refreshTimer.current) clearTimeout(refreshTimer.current);
    const expiry = parseJwtExpiry(accessToken);
    if (!expiry) return;
    const delay = expiry - Date.now() - REFRESH_BEFORE_MS;
    if (delay <= 0) {
      doRefresh();
      return;
    }
    refreshTimer.current = setTimeout(doRefresh, delay);
  };

  const doRefresh = async () => {
    const refresh = localStorage.getItem("refresh");
    if (!refresh) return;
    try {
      const res = await API.post("token/refresh/", { refresh });
      localStorage.setItem("access", res.data.access);
      if (res.data.refresh) localStorage.setItem("refresh", res.data.refresh);
      scheduleRefresh(res.data.access);
    } catch {
      logout();
    }
  };

  const getUser = async () => {
    const token = localStorage.getItem("access");
    if (!token) return;
    try {
      const res = await API.get("me/");
      setUser(res.data);
      scheduleRefresh(token);
    } catch {
      localStorage.removeItem("access");
      localStorage.removeItem("refresh");
      setUser(null);
    }
  };

  const login = async (username, password) => {
    const res = await API.post("token/", { username, password });
    localStorage.setItem("access", res.data.access);
    localStorage.setItem("refresh", res.data.refresh);
    await getUser();
  };

  const register = async (data) => {
    await API.post("register/", data);
  };

  const logout = () => {
    if (refreshTimer.current) clearTimeout(refreshTimer.current);
    localStorage.removeItem("access");
    localStorage.removeItem("refresh");
    setUser(null);
  };

  useEffect(() => {
    const token = localStorage.getItem("access");
    if (token) {
      getUser().finally(() => setReady(true));
    } else {
      setReady(true);
    }
    return () => {
      if (refreshTimer.current) clearTimeout(refreshTimer.current);
    };
  }, []);

  // Don't render children until we know auth state
  if (!ready) return null;

  return (
    <AuthContext.Provider value={{ user, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
};