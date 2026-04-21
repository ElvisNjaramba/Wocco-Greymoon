import axios from "axios";

const BASE_URL = "https://greymoonignorelistcom.dbm.shared-servers.com/api/";

const API = axios.create({
  baseURL: BASE_URL,
});

API.interceptors.request.use(
  config => {
    const token = localStorage.getItem("access");
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  error => Promise.reject(error)
);

let _refreshing = null;
let _lastRefreshAt = 0;

API.interceptors.response.use(
  response => response,
  async error => {
    const original = error.config;

    if (error.response?.status === 401 && !original._retry) {
      original._retry = true;

      const refresh = localStorage.getItem("refresh");
      if (!refresh) {
        localStorage.removeItem("access");
        localStorage.removeItem("refresh");
        window.location.href = "/login";
        return Promise.reject(error);
      }

      if (!_refreshing) {
        const now = Date.now();

        // If we refreshed successfully in the last 5 seconds,
        // just use the token already in storage — don't hit the server again
        if (now - _lastRefreshAt < 5000) {
          const freshAccess = localStorage.getItem("access");
          if (freshAccess) {
            original.headers.Authorization = `Bearer ${freshAccess}`;
            return API(original);
          }
        }

        _refreshing = axios
          .post(`${BASE_URL}token/refresh/`, { refresh })
          .then(res => {
            _lastRefreshAt = Date.now();
            localStorage.setItem("access", res.data.access);
            if (res.data.refresh) {
              localStorage.setItem("refresh", res.data.refresh);
            }
            return res.data.access;
          })
          .catch(() => {
            localStorage.removeItem("access");
            localStorage.removeItem("refresh");
            window.location.href = "/login";
            return null;
          })
          .finally(() => {
            _refreshing = null;
          });
      }

      const newAccess = await _refreshing;
      if (newAccess) {
        original.headers.Authorization = `Bearer ${newAccess}`;
        return API(original);
      }
    }

    return Promise.reject(error);
  }
);

export default API;