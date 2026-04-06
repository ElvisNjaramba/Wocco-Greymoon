import axios from "axios";

const API = axios.create({
  baseURL: "https://greymoonignorelistcom.dbm.shared-servers.com/api/",
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

      // Prevent multiple simultaneous refresh calls
      if (!_refreshing) {
        _refreshing = axios
          .post(
            "https://greymoonignorelistcom.dbm.shared-servers.com/api/token/refresh/",
            { refresh }
          )
          .then(res => {
            localStorage.setItem("access", res.data.access);
            // If rotation is enabled, update refresh token too
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