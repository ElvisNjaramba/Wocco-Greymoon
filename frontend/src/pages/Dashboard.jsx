import React, { useEffect, useState, useContext } from "react";
import axios from "axios";
import { AuthContext } from "../context/AuthContext";
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";

const Services = () => {
  const { logout, user } = useContext(AuthContext);

  const [services, setServices] = useState([]);
  const [loading, setLoading] = useState(false);
  const [scraping, setScraping] = useState(false);
  const [runId, setRunId] = useState(null);
  const [leadCount, setLeadCount] = useState(0);


  const token = localStorage.getItem("access");

  // Fetch services
  const fetchServices = async () => {
    try {
      const response = await axios.get(
        "http://127.0.0.1:8000/api/services/",
        { headers: { Authorization: `Bearer ${token}` } }
      );

      setServices(response.data);
      setLeadCount(response.data.length);
    } catch (err) {
      console.error("Error fetching services:", err);
    }
  };

  // Check scrape status
  const checkStatus = async () => {
    try {
      const res = await axios.get(
        "http://127.0.0.1:8000/api/scrape-status/",
        { headers: { Authorization: `Bearer ${token}` } }
      );

      if (res.data.status === "RUNNING") {
        setScraping(true);
        setRunId(res.data.run_id);
      } else {
        setScraping(false);
      }
    } catch (err) {
      console.error("Status check failed");
    }
  };

  // Start scrape
  const triggerScrape = async () => {
    setScraping(true);
    try {
      const res = await axios.post(
        "http://127.0.0.1:8000/api/scrape-services/",
        {},
        { headers: { Authorization: `Bearer ${token}` } }
      );
      setRunId(res.data.run_id);
    } catch (err) {
      console.error("Error running scraper:", err);
      setScraping(false);
    }
  };

  // Cancel scrape
  const cancelScrape = async () => {
    try {
      await axios.post(
        "http://127.0.0.1:8000/api/cancel-scrape/",
        { run_id: runId },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      setScraping(false);
    } catch (err) {
      console.error("Cancel failed");
    }
  };

  const mapLeads = services.filter(
    (s) => s.latitude && s.longitude
  );


  useEffect(() => {
    fetchServices();
    checkStatus();

    const interval = setInterval(() => {
      fetchServices();
      checkStatus();
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="p-4">
      <div className="flex justify-between items-center mb-4">
        <h1 className="text-2xl font-bold">Scraped Services</h1>
        {user && (
          <button
            onClick={logout}
            className="bg-red-500 text-white px-4 py-2 rounded"
          >
            Logout
          </button>
        )}
      </div>

      {!scraping ? (
        <button
          onClick={triggerScrape}
          className="bg-blue-600 text-white px-4 py-2 rounded mb-4"
        >
          Start Scraping
        </button>
      ) : (
        <div className="mb-4">
          <button
            className="bg-yellow-500 text-white px-4 py-2 rounded mr-3"
            disabled
          >
            Scraping in progress...
          </button>
          <button
            onClick={cancelScrape}
            className="bg-red-600 text-white px-4 py-2 rounded"
          >
            Cancel Scrape
          </button>
        </div>
      )}

      <div className="flex items-center mb-3">
        <div className="text-lg font-semibold">
          Leads Collected:{" "}
          <span className="text-blue-600">{leadCount}</span>
        </div>

        {scraping && (
          <div className="ml-4 text-yellow-600 animate-pulse">
            Scraping live...
          </div>
        )}
      </div>


      {loading ? (
        <p>Loading...</p>
      ) : (
        <div className="overflow-auto">
          <table className="w-full table-auto border-collapse border border-gray-300 text-sm">
            <thead>
              <tr className="bg-gray-100">
                <th>Title</th>
                <th>Category</th>
                <th>Location</th>
                <th>Phone</th>
                <th>Email</th>
                <th>Zip</th>
                <th>Longitude</th>
                <th>Latitude</th>
                <th>Label</th>
                <th>Date</th>
              </tr>
            </thead>
            <tbody>
              {services.map((s) => (
                <tr key={s.post_id} className="border-t">
                  <td>{s.title}</td>
                  <td>{s.category}</td>
                  <td>{s.location}</td>
                  <td>{s.phone}</td>
                  <td>{s.email}</td>
                  <td>{s.zip_code}</td>
                  <td>{s.longitude}</td>
                  <td>{s.latitude}</td>
                  <td>{s.label}</td>
                  <td>
                    {s.datetime
                      ? new Date(s.datetime).toLocaleString()
                      : "N/A"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div className="mt-8">
  <h2 className="text-xl font-bold mb-3">Lead Locations</h2>

  <MapContainer
    center={[39.5, -98.35]} // Center of US
    zoom={4}
    style={{ height: "500px", width: "100%" }}
  >
    <TileLayer
      attribution='&copy; OpenStreetMap contributors'
      url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
    />

    {mapLeads.map((lead) => (
      <Marker
        key={lead.post_id}
        position={[
          parseFloat(lead.latitude),
          parseFloat(lead.longitude),
        ]}
      >
        <Popup>
          <strong>{lead.title}</strong>
          <br />
          {lead.location}
          <br />
          {lead.phone}
          <br />
          {lead.email}
        </Popup>
      </Marker>
    ))}
  </MapContainer>
</div>

    </div>
  );
};

export default Services;
