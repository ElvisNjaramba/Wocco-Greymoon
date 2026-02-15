import { createContext, useState, useEffect } from "react";
import API from "../api/axios";

export const AuthContext = createContext();

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);

const login = async (username, password) => {
  const res = await API.post("token/", { username, password });

  localStorage.setItem("access", res.data.access);
  localStorage.setItem("refresh", res.data.refresh);

  await getUser();
};


  const register = async (data) => {
    await API.post("register/", data);
  };

const getUser = async () => {
  const token = localStorage.getItem("access");
  if (!token) return;

  try {
    const res = await API.get("me/");
    setUser(res.data);
  } catch (err) {
    localStorage.removeItem("access");
    localStorage.removeItem("refresh");
    setUser(null);
  }
};


  const logout = () => {
    localStorage.removeItem("access");
    localStorage.removeItem("refresh");
    setUser(null);
  };

useEffect(() => {
  const token = localStorage.getItem("access");
  if (token) {
    getUser();
  }
}, []);


  return (
    <AuthContext.Provider value={{ user, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
};
