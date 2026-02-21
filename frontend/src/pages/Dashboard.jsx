import React, { useEffect, useState, useContext, useMemo } from "react";
import axios from "axios";
import { AuthContext } from "../context/AuthContext";
import { MapContainer, TileLayer, Marker, Popup, Tooltip } from "react-leaflet";
import { Phone, Mail, MapPin, Calendar, Star, X, RefreshCw, Download, Filter, Eye, CheckSquare, Square, ChevronDown } from 'lucide-react';

const Services = () => {
  const { logout, user } = useContext(AuthContext);

  const [services, setServices] = useState([]);
  const [filtered, setFiltered] = useState([]);
  const [loading, setLoading] = useState(false);
  const [scraping, setScraping] = useState(false);
  const [runId, setRunId] = useState(null);
  const [leadCount, setLeadCount] = useState(0);
  const [selectedLead, setSelectedLead] = useState(null);
  const [history, setHistory] = useState([]);
  const [selectedCities, setSelectedCities] = useState([]);
  const [isCityDropdownOpen, setIsCityDropdownOpen] = useState(false);
  const [availableCities, setAvailableCities] = useState([]);
  const [loadingCities, setLoadingCities] = useState(false);
  const [isAborting, setIsAborting] = useState(false);

  // Filter states
  const [filterCategory, setFilterCategory] = useState("");
  const [filterHasPhone, setFilterHasPhone] = useState(false);
  const [filterHasEmail, setFilterHasEmail] = useState(false);
  const [filterState, setFilterState] = useState("");
  const [filterStatus, setFilterStatus] = useState("");
  const [filterMinScore, setFilterMinScore] = useState("");
  const [filterMaxScore, setFilterMaxScore] = useState("");

  // Pagination
  const [currentPage, setCurrentPage] = useState(1);
  const leadsPerPage = 20;

  const token = localStorage.getItem("access");

  // Fetch all leads
  const fetchServices = async () => {
    setLoading(true);
    try {
      const response = await axios.get(
        "http://127.0.0.1:8000/api/services/",
        { headers: { Authorization: `Bearer ${token}` } }
      );
      setServices(response.data);
      setFiltered(response.data);
      setLeadCount(response.data.length);
    } catch (err) {
      console.error("Error fetching services:", err);
    }
    setLoading(false);
  };

  // Fetch cities with structure
  const fetchCities = async () => {
    setLoadingCities(true);
    try {
      const response = await axios.get(
        "http://127.0.0.1:8000/api/cities/",
        { headers: { Authorization: `Bearer ${token}` } }
      );
      setAvailableCities(response.data.cities); // array of {code, name, state, display}
    } catch (err) {
      console.error("Error fetching cities:", err);
    } finally {
      setLoadingCities(false);
    }
  };

  // Group cities by state for dropdown
  const citiesByState = useMemo(() => {
    return availableCities.reduce((acc, city) => {
      const state = city.state || 'Other';
      if (!acc[state]) acc[state] = [];
      acc[state].push(city);
      return acc;
    }, {});
  }, [availableCities]);

  // Scraper status
  const checkStatus = async () => {
    try {
      const res = await axios.get(
        "http://127.0.0.1:8000/api/scrape-status/",
        { headers: { Authorization: `Bearer ${token}` } }
      );
      if (res.data.status === "RUNNING") {
        setScraping(true);
      } else {
        setScraping(false);
        setIsAborting(false);
      }
      setRunId(res.data.run_id);
    } catch (err) {
      console.error("Status check failed");
    }
  };

  // Start scraping
  const triggerScrape = async () => {
    if (selectedCities.length === 0) {
      alert("Please select at least one city.");
      return;
    }
    setScraping(true);
    try {
      const res = await axios.post(
        "http://127.0.0.1:8000/api/scrape-services/",
        { cities: selectedCities },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      setRunId(res.data.run_id);
    } catch (err) {
      console.error("Error running scraper:", err);
      setScraping(false);
    }
  };

  // Cancel scraping
  const cancelScrape = async () => {
    if (!runId) return;
    try {
      setIsAborting(true);
      await axios.post(
        "http://127.0.0.1:8000/api/cancel-scrape/",
        { run_id: runId },
        { headers: { Authorization: `Bearer ${token}` } }
      );
    } catch (err) {
      console.error("Cancel failed");
      setIsAborting(false);
    }
  };

  // Enhanced filter handler
  const applyFilters = () => {
    let temp = [...services];

    if (filterCategory) {
      temp = temp.filter((s) => s.category === filterCategory);
    }
    if (filterHasPhone) {
      temp = temp.filter((s) => s.phone);
    }
    if (filterHasEmail) {
      temp = temp.filter((s) => s.email);
    }
    if (filterState) {
      temp = temp.filter((s) => s.state === filterState);
    }
    if (filterStatus) {
      temp = temp.filter((s) => s.status === filterStatus);
    }
    if (filterMinScore) {
      temp = temp.filter((s) => s.score >= parseInt(filterMinScore, 10));
    }
    if (filterMaxScore) {
      temp = temp.filter((s) => s.score <= parseInt(filterMaxScore, 10));
    }

    setFiltered(temp);
    setCurrentPage(1);
  };

  // Reset filters
  const resetFilters = () => {
    setFilterCategory("");
    setFilterHasPhone(false);
    setFilterHasEmail(false);
    setFilterState("");
    setFilterStatus("");
    setFilterMinScore("");
    setFilterMaxScore("");
    setFiltered(services);
    setCurrentPage(1);
  };

  // Fetch history
  const fetchHistory = async () => {
    try {
      const res = await axios.get(
        "http://127.0.0.1:8000/api/scrape-history/",
        { headers: { Authorization: `Bearer ${token}` } }
      );
      setHistory(res.data);
    } catch (err) {
      console.error("Error fetching history:", err);
    }
  };

  const usaBounds = [
    [24.396308, -124.848974],
    [49.384358, -66.885444]
  ];

  const statusCounts = useMemo(() => {
    return {
      NEW: services.filter(s => s.status === "NEW").length,
      CONTACTED: services.filter(s => s.status === "CONTACTED").length,
      QUALIFIED: services.filter(s => s.status === "QUALIFIED").length,
      WON: services.filter(s => s.status === "WON").length,
      LOST: services.filter(s => s.status === "LOST").length,
    };
  }, [services]);

  // Pagination
  const indexOfLastLead = currentPage * leadsPerPage;
  const indexOfFirstLead = indexOfLastLead - leadsPerPage;
  const currentLeads = filtered.slice(indexOfFirstLead, indexOfLastLead);
  const totalPages = Math.ceil(filtered.length / leadsPerPage);
  const paginate = (pageNum) => setCurrentPage(pageNum);

  const mapLeads = filtered.filter((s) => s.latitude && s.longitude);

  // History pagination
  const [historyPage, setHistoryPage] = useState(1);
  const historyPerPage = 10;
  const indexOfLastHistory = historyPage * historyPerPage;
  const indexOfFirstHistory = indexOfLastHistory - historyPerPage;
  const currentHistory = history.slice(indexOfFirstHistory, indexOfLastHistory);
  const totalHistoryPages = Math.ceil(history.length / historyPerPage);
  const paginateHistory = (pageNum) => setHistoryPage(pageNum);

  useEffect(() => {
    fetchServices();
    checkStatus();
    fetchHistory();
    fetchCities();
  }, []);

  useEffect(() => {
    if (!scraping) return;
    const interval = setInterval(() => {
      fetchServices();
      checkStatus();
    }, 5000);
    return () => clearInterval(interval);
  }, [scraping]);

  // Apply filters whenever any filter changes
  useEffect(() => {
    applyFilters();
  }, [filterCategory, filterHasPhone, filterHasEmail, filterState, filterStatus, filterMinScore, filterMaxScore, services]);

  // Color helpers
  const getScoreColor = (score) => {
    if (score >= 70) return 'bg-gradient-to-r from-green-400 to-green-500';
    if (score >= 40) return 'bg-gradient-to-r from-yellow-400 to-yellow-500';
    return 'bg-gradient-to-r from-gray-400 to-gray-500';
  };

  const getStatusColor = (status) => {
    const colors = {
      'NEW': 'bg-blue-100 text-blue-800 border-blue-200',
      'CONTACTED': 'bg-yellow-100 text-yellow-800 border-yellow-200',
      'QUALIFIED': 'bg-green-100 text-green-800 border-green-200',
      'WON': 'bg-purple-100 text-purple-800 border-purple-200',
      'LOST': 'bg-red-100 text-red-800 border-red-200'
    };
    return colors[status] || 'bg-gray-100 text-gray-800 border-gray-200';
  };

  // City selection helpers
  const toggleCity = (cityCode) => {
    setSelectedCities(prev =>
      prev.includes(cityCode)
        ? prev.filter(c => c !== cityCode)
        : [...prev, cityCode]
    );
  };

  const selectAllCities = () => {
    setSelectedCities(availableCities.map(c => c.code));
  };

  const clearAllCities = () => {
    setSelectedCities([]);
  };

  const formatCityName = (cityCode) => {
    const cityObj = availableCities.find(c => c.code === cityCode);
    return cityObj ? cityObj.display : cityCode;
  };

  // Get unique states from services for filter dropdown
  const uniqueStates = useMemo(() => {
    return [...new Set(services.map(s => s.state).filter(Boolean))].sort();
  }, [services]);

  // Get unique statuses from services
  const uniqueStatuses = useMemo(() => {
    return [...new Set(services.map(s => s.status))];
  }, [services]);

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
      {/* Header (unchanged) */}
      <div className="bg-white/80 backdrop-blur-md shadow-lg sticky top-0 z-40 border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex justify-between items-center">
            <div className="flex items-center space-x-4">
              <div className="bg-gradient-to-r from-blue-600 to-blue-700 p-2 rounded-lg shadow-lg">
                <RefreshCw className="w-6 h-6 text-white" />
              </div>
              <div>
                <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-600 to-blue-800 bg-clip-text text-transparent">
                  Wocco Greymoon Scraper
                </h1>
                <p className="text-sm text-gray-500">Manage and track your leads</p>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2 bg-blue-50 px-4 py-2 rounded-lg">
                <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
                <span className="text-sm font-medium text-gray-700">{user?.username || 'User'}</span>
              </div>
              <button
                onClick={logout}
                className="bg-gradient-to-r from-red-500 to-red-600 text-white px-4 py-2 rounded-lg font-semibold hover:from-red-600 hover:to-red-700 transition-all duration-200 shadow-md hover:shadow-lg flex items-center space-x-2"
              >
                <span>Logout</span>
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Stats Cards (unchanged) */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          {[
            { label: 'Total Leads', value: leadCount, color: 'from-blue-500 to-blue-600', icon: RefreshCw },
            { label: 'With Phone', value: services.filter(s => s.phone).length, color: 'from-green-500 to-green-600', icon: Phone },
            { label: 'With Email', value: services.filter(s => s.email).length, color: 'from-yellow-500 to-yellow-600', icon: Mail },
            { label: 'With Location', value: mapLeads.length, color: 'from-purple-500 to-purple-600', icon: MapPin }
          ].map((stat, index) => (
            <div key={index} className="bg-white rounded-xl shadow-lg overflow-hidden hover:shadow-xl transition-shadow duration-300">
              <div className={`bg-gradient-to-r ${stat.color} px-4 py-2`}>
                <stat.icon className="w-5 h-5 text-white" />
              </div>
              <div className="p-4">
                <p className="text-sm text-gray-600 uppercase tracking-wider">{stat.label}</p>
                <p className="text-3xl font-bold text-gray-800">{stat.value}</p>
              </div>
            </div>
          ))}
        </div>

        {/* Scrape Controls Card (unchanged) */}
        <div className="bg-white rounded-xl shadow-lg mb-8 relative">
          <div className="bg-gradient-to-r from-gray-50 to-gray-100 px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-800 flex items-center">
              <RefreshCw className="w-5 h-5 mr-2 text-blue-600" />
              Scrape Controls
            </h2>
          </div>
          <div className="p-6">
            {/* City Selection (unchanged) */}
            <div className="mb-6">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Select Cities to Scrape
              </label>
              <div className="relative">
                <button
                  type="button"
                  onClick={() => setIsCityDropdownOpen(!isCityDropdownOpen)}
                  disabled={loadingCities}
                  className="w-full border border-gray-300 rounded-lg px-4 py-3 text-left focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-white hover:bg-gray-50 transition-colors flex items-center justify-between disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <div className="flex items-center space-x-2 truncate">
                    {loadingCities ? (
                      <span className="text-gray-500">Loading cities...</span>
                    ) : selectedCities.length === 0 ? (
                      <span className="text-gray-500">Choose cities to scrape...</span>
                    ) : (
                      <>
                        <div className="bg-blue-100 text-blue-800 px-2 py-0.5 rounded-full text-xs font-medium">
                          {selectedCities.length} selected
                        </div>
                        <span className="text-gray-600 truncate">
                          {selectedCities.slice(0, 3).map(formatCityName).join(', ')}
                          {selectedCities.length > 3 && ` +${selectedCities.length - 3} more`}
                        </span>
                      </>
                    )}
                  </div>
                  <ChevronDown className={`w-5 h-5 text-gray-400 transition-transform duration-200 ${isCityDropdownOpen ? 'transform rotate-180' : ''}`} />
                </button>

                {isCityDropdownOpen && !loadingCities && (
                  <div className="absolute left-0 right-0 z-50 mt-1 bg-white border-2 border-blue-200 rounded-lg shadow-2xl max-h-[500px] overflow-y-auto">
                    <div className="sticky top-0 bg-gradient-to-r from-blue-50 to-blue-100 px-4 py-3 border-b-2 border-blue-200 flex justify-between items-center">
                      <span className="text-sm font-bold text-blue-800">
                        {availableCities.length} CITIES AVAILABLE
                      </span>
                      <div className="space-x-3">
                        <button onClick={selectAllCities} className="text-xs bg-blue-600 text-white px-3 py-1.5 rounded-md hover:bg-blue-700 font-medium shadow-sm">
                          Select All
                        </button>
                        <button onClick={clearAllCities} className="text-xs bg-gray-600 text-white px-3 py-1.5 rounded-md hover:bg-gray-700 font-medium shadow-sm">
                          Clear
                        </button>
                      </div>
                    </div>
                    <div className="p-4 bg-white">
                      {Object.entries(citiesByState)
                        .sort(([stateA], [stateB]) => stateA.localeCompare(stateB))
                        .map(([state, cities]) => (
                          <div key={state} className="mb-6">
                            <h4 className="font-bold text-gray-700 mb-2 px-2 border-b border-gray-200 pb-1">
                              {state}
                            </h4>
                            <div className="grid grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-x-6 gap-y-3">
                              {cities.map((cityObj) => (
                                <label
                                  key={cityObj.code}
                                  className="flex items-center space-x-3 px-3 py-2.5 hover:bg-blue-50 rounded-lg cursor-pointer transition-colors border border-transparent hover:border-blue-200"
                                >
                                  <input
                                    type="checkbox"
                                    checked={selectedCities.includes(cityObj.code)}
                                    onChange={() => toggleCity(cityObj.code)}
                                    className="w-4 h-4 text-blue-600 rounded focus:ring-blue-500"
                                  />
                                  <span className="text-gray-800 font-medium">{cityObj.display}</span>
                                </label>
                              ))}
                            </div>
                          </div>
                        ))}
                    </div>
                    <div className="sticky bottom-0 bg-gray-50 px-4 py-2 border-t-2 border-gray-200 text-xs text-gray-600 text-center">
                      Showing {availableCities.length} cities • Select multiple cities to scrape
                    </div>
                  </div>
                )}
              </div>

              {/* Selected cities chips */}
              {selectedCities.length > 0 && (
                <div className="mt-3 flex flex-wrap gap-2 max-h-32 overflow-y-auto p-2 bg-gray-50 rounded-lg border border-gray-200">
                  {selectedCities.map((code) => {
                    const cityObj = availableCities.find(c => c.code === code);
                    return (
                      <span
                        key={code}
                        className="inline-flex items-center bg-blue-100 text-blue-800 px-3 py-1.5 rounded-full text-sm font-medium border border-blue-300 shadow-sm"
                      >
                        <span>{cityObj ? cityObj.display : formatCityName(code)}</span>
                        <button
                          onClick={() => toggleCity(code)}
                          className="ml-2 text-blue-600 hover:text-blue-800 hover:bg-blue-200 rounded-full p-0.5"
                        >
                          <X className="w-3.5 h-3.5" />
                        </button>
                      </span>
                    );
                  })}
                </div>
              )}
            </div>

            {/* Scrape Buttons (unchanged) */}
            <div className="flex items-center justify-between flex-wrap gap-4">
              <div>
                {!scraping ? (
                  <button
                    onClick={triggerScrape}
                    disabled={selectedCities.length === 0 || loadingCities}
                    className={`bg-gradient-to-r from-green-500 to-green-600 hover:from-green-600 hover:to-green-700 text-white px-6 py-3 rounded-lg font-semibold transition-all duration-200 shadow-md hover:shadow-lg flex items-center space-x-2 ${selectedCities.length === 0 || loadingCities ? 'opacity-50 cursor-not-allowed' : ''
                      }`}
                  >
                    <RefreshCw className="w-5 h-5" />
                    <span>Start Scraping</span>
                  </button>
                ) : (
                  <div className="flex items-center space-x-4 flex-wrap gap-3">
                    <div className="flex items-center space-x-3 bg-yellow-50 px-4 py-2 rounded-lg border border-yellow-200">
                      <div className="animate-spin rounded-full h-5 w-5 border-2 border-yellow-500 border-t-transparent"></div>
                      <span className="text-yellow-700 font-medium">
                        {isAborting ? "Aborting scrape..." : "Scraping in progress..."}
                      </span>
                    </div>
                    <button
                      onClick={cancelScrape}
                      disabled={isAborting}
                      className={`bg-gradient-to-r from-red-500 to-red-600 text-white px-4 py-2 rounded-lg font-semibold transition-all duration-200 shadow-md hover:shadow-lg ${isAborting ? "opacity-50 cursor-not-allowed" : ""}`}
                    >
                      {isAborting ? "Aborting..." : "Cancel Scrape"}
                    </button>
                  </div>
                )}
              </div>
              {scraping && runId && (
                <div className="text-sm bg-gray-50 px-4 py-2 rounded-lg border border-gray-200">
                  <span className="text-gray-600">Run ID:</span>
                  <span className="ml-2 font-mono text-gray-800">{runId}</span>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* ========== COMPACT STATUS PILLS + FILTERS ========== */}
        {/* Status pills - minimal space */}
        <div className="flex flex-wrap gap-2 mb-4">
          {[
            { status: 'NEW', color: 'blue', count: statusCounts.NEW },
            { status: 'CONTACTED', color: 'yellow', count: statusCounts.CONTACTED },
            { status: 'QUALIFIED', color: 'green', count: statusCounts.QUALIFIED },
            { status: 'WON', color: 'purple', count: statusCounts.WON },
            { status: 'LOST', color: 'red', count: statusCounts.LOST },
          ].map(({ status, color, count }) => (
            <div
              key={status}
              className={`inline-flex items-center bg-${color}-50 text-${color}-700 px-3 py-1 rounded-full text-sm font-medium border border-${color}-200`}
            >
              <span>{status}</span>
              <span className={`ml-2 bg-${color}-200 text-${color}-800 px-2 py-0.5 rounded-full text-xs font-bold`}>
                {count}
              </span>
            </div>
          ))}
        </div>

        {/* Filters Card - Full width */}
        <div className="bg-white rounded-xl shadow-lg overflow-hidden mb-8">
          <div className="bg-gradient-to-r from-gray-50 to-gray-100 px-4 py-3 border-b border-gray-200 flex justify-between items-center">
            <h3 className="font-semibold text-gray-800 flex items-center">
              <Filter className="w-4 h-4 mr-2 text-blue-600" />
              Filters
            </h3>
            <button
              onClick={resetFilters}
              className="text-xs text-blue-600 hover:text-blue-800 font-medium"
            >
              Reset
            </button>
          </div>
          <div className="p-4 space-y-4">
            {/* Category */}
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Category</label>
              <select
                value={filterCategory}
                onChange={(e) => setFilterCategory(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">All Categories</option>
                {[...new Set(services.map((s) => s.category))].map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>

            {/* State */}
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">State</label>
              <select
                value={filterState}
                onChange={(e) => setFilterState(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">All States</option>
                {uniqueStates.map(state => (
                  <option key={state} value={state}>{state}</option>
                ))}
              </select>
            </div>

            {/* Status */}
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Status</label>
              <select
                value={filterStatus}
                onChange={(e) => setFilterStatus(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">All Statuses</option>
                {uniqueStatuses.map(status => (
                  <option key={status} value={status}>{status}</option>
                ))}
              </select>
            </div>

            {/* Score Range */}
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Score Range</label>
              <div className="flex items-center space-x-2">
                <input
                  type="number"
                  min="0"
                  max="100"
                  placeholder="Min"
                  value={filterMinScore}
                  onChange={(e) => setFilterMinScore(e.target.value)}
                  className="w-1/2 border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
                <span className="text-gray-500">-</span>
                <input
                  type="number"
                  min="0"
                  max="100"
                  placeholder="Max"
                  value={filterMaxScore}
                  onChange={(e) => setFilterMaxScore(e.target.value)}
                  className="w-1/2 border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
            </div>

            {/* Checkboxes */}
            <div className="space-y-2 pt-2">
              <label className="flex items-center space-x-3 cursor-pointer p-2 hover:bg-gray-50 rounded-lg transition-colors">
                <input
                  type="checkbox"
                  checked={filterHasPhone}
                  onChange={(e) => setFilterHasPhone(e.target.checked)}
                  className="w-4 h-4 text-blue-600 rounded focus:ring-blue-500"
                />
                <span className="text-gray-700 text-sm">Has Phone</span>
              </label>

              <label className="flex items-center space-x-3 cursor-pointer p-2 hover:bg-gray-50 rounded-lg transition-colors">
                <input
                  type="checkbox"
                  checked={filterHasEmail}
                  onChange={(e) => setFilterHasEmail(e.target.checked)}
                  className="w-4 h-4 text-blue-600 rounded focus:ring-blue-500"
                />
                <span className="text-gray-700 text-sm">Has Email</span>
              </label>
            </div>
          </div>
        </div>

        {/* Leads Table (unchanged) */}
        <div className="bg-white rounded-xl shadow-lg mb-8 overflow-hidden">
          <div className="bg-gradient-to-r from-gray-50 to-gray-100 px-6 py-4 border-b border-gray-200 flex justify-between items-center">
            <h2 className="text-lg font-semibold text-gray-800">Leads</h2>
            <div className="text-sm bg-blue-100 text-blue-800 px-3 py-1 rounded-full">
              Showing {indexOfFirstLead + 1}-{Math.min(indexOfLastLead, filtered.length)} of {filtered.length}
            </div>
          </div>

          {loading ? (
            <div className="p-12 text-center">
              <div className="inline-block animate-spin rounded-full h-8 w-8 border-3 border-blue-600 border-t-transparent"></div>
              <p className="mt-3 text-gray-600 font-medium">Loading leads...</p>
            </div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Title</th>
                      <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Category</th>
                      <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">State</th>
                      <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Zip</th>
                      <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Contact</th>
                      <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Status</th>
                      <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Score</th>
                      <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Date</th>
                      <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {currentLeads.map((s) => (
                      <tr key={s.post_id} className="hover:bg-gray-50 transition-colors group">
                        <td className="px-6 py-4">
                          <div className="font-medium text-gray-900">{s.title}</div>
                        </td>
                        <td className="px-6 py-4">
                          <span className="bg-blue-50 text-blue-700 px-3 py-1 rounded-full text-xs font-medium border border-blue-200">
                            {s.category}
                          </span>
                        </td>
                        <td className="px-6 py-4 text-sm text-gray-600">{s.state || 'N/A'}</td>
                        <td className="px-6 py-4 text-sm text-gray-600">{s.zip_code || 'N/A'}</td>
                        <td className="px-6 py-4">
                          <div className="flex space-x-2">
                            {s.phone && <Phone className="w-4 h-4 text-green-600" />}
                            {s.email && <Mail className="w-4 h-4 text-blue-600" />}
                          </div>
                        </td>
                        <td className="px-6 py-4">
                          <select
                            value={s.status}
                            onChange={async (e) => {
                              await axios.patch(
                                `http://127.0.0.1:8000/api/leads/${s.post_id}/status/`,
                                { status: e.target.value },
                                { headers: { Authorization: `Bearer ${token}` } }
                              );
                              fetchServices();
                            }}
                            className={`border rounded-lg px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 ${getStatusColor(s.status)}`}
                            onClick={(e) => e.stopPropagation()}
                          >
                            <option value="NEW">New</option>
                            <option value="CONTACTED">Contacted</option>
                            <option value="QUALIFIED">Qualified</option>
                            <option value="WON">Won</option>
                            <option value="LOST">Lost</option>
                          </select>
                        </td>
                        <td className="px-6 py-4">
                          <span className={`px-3 py-1 rounded-full text-xs font-semibold text-white ${getScoreColor(s.score)}`}>
                            {s.score}
                          </span>
                        </td>
                        <td className="px-6 py-4 text-sm text-gray-600">
                          <div className="flex items-center">
                            <Calendar className="w-4 h-4 mr-1 text-gray-400" />
                            {s.datetime ? new Date(s.datetime).toLocaleDateString() : 'N/A'}
                          </div>
                        </td>
                        <td className="px-6 py-4">
                          <button
                            onClick={() => setSelectedLead(s)}
                            className="text-blue-600 hover:text-blue-800 transition-colors"
                          >
                            <Eye className="w-5 h-5" />
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="px-6 py-4 border-t border-gray-200 flex justify-center">
                  <div className="flex space-x-2">
                    {Array.from({ length: totalPages }, (_, i) => (
                      <button
                        key={i}
                        onClick={() => paginate(i + 1)}
                        className={`px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${currentPage === i + 1
                          ? 'bg-gradient-to-r from-blue-500 to-blue-600 text-white shadow-md'
                          : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                          }`}
                      >
                        {i + 1}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}
        </div>

        {/* Scrape History (unchanged) */}
        <div className="bg-white rounded-xl shadow-lg mb-8 overflow-hidden">
          <div className="bg-gradient-to-r from-gray-50 to-gray-100 px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-800">Scrape History</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Run ID</th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Status</th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Leads</th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Started</th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider">Finished</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {currentHistory.map((h) => (
                  <tr key={h.run_id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 font-mono text-sm text-gray-600">{h.run_id.substring(0, 8)}...</td>
                    <td className="px-6 py-4">
                      <span className={`px-3 py-1 rounded-full text-xs font-semibold ${h.status === 'COMPLETED'
                        ? 'bg-green-100 text-green-800 border border-green-200'
                        : h.status === 'RUNNING'
                          ? 'bg-yellow-100 text-yellow-800 border border-yellow-200 animate-pulse'
                          : 'bg-red-100 text-red-800 border border-red-200'
                        }`}>
                        {h.status}
                      </span>
                    </td>
                    <td className="px-6 py-4 font-medium">{h.leads_collected}</td>
                    <td className="px-6 py-4 text-sm text-gray-600">{new Date(h.started_at).toLocaleString()}</td>
                    <td className="px-6 py-4 text-sm text-gray-600">
                      {h.finished_at ? new Date(h.finished_at).toLocaleString() : 'In progress...'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {totalHistoryPages > 1 && (
              <div className="px-6 py-4 border-t border-gray-200 flex justify-center">
                <div className="flex space-x-2">
                  {Array.from({ length: totalHistoryPages }, (_, i) => (
                    <button
                      key={i}
                      onClick={() => paginateHistory(i + 1)}
                      className={`px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${historyPage === i + 1
                        ? 'bg-gradient-to-r from-blue-500 to-blue-600 text-white shadow-md'
                        : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                        }`}
                    >
                      {i + 1}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Map (unchanged) */}
        <div className="bg-white rounded-xl shadow-lg overflow-hidden">
          <div className="bg-gradient-to-r from-gray-50 to-gray-100 px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-800 flex items-center">
              <MapPin className="w-5 h-5 mr-2 text-blue-600" />
              Lead Locations
            </h2>
          </div>
          <div className="p-2">
            <MapContainer
              center={[37.8, -96]}
              zoom={4}
              minZoom={4}
              maxZoom={10}
              scrollWheelZoom={true}
              style={{ height: "500px", width: "100%", borderRadius: "0.5rem" }}
              maxBounds={usaBounds}
              maxBoundsViscosity={1.0}
              whenCreated={(map) => {
                map.setMaxBounds(usaBounds);
                map.fitBounds(usaBounds);
              }}
            >
              <TileLayer
                attribution='&copy; OpenStreetMap contributors'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              />
{mapLeads.map((lead) => (
  <Marker
    key={lead.post_id}
    position={[parseFloat(lead.latitude), parseFloat(lead.longitude)]}
    eventHandlers={{ click: () => setSelectedLead(lead) }}
  >
    <Tooltip>
      <div className="font-semibold text-gray-900">{lead.title}</div>
      {lead.state && <div className="text-sm text-gray-600">State: {lead.state}</div>}
      {lead.zip_code && <div className="text-sm text-gray-600">Zip: {lead.zip_code}</div>}
      {lead.phone && <div className="text-sm text-gray-600 mt-1">📞 {lead.phone}</div>}
      {lead.email && <div className="text-sm text-gray-600">✉️ {lead.email}</div>}
    </Tooltip>
  </Marker>
))}
            </MapContainer>
          </div>
        </div>
      </div>

      {/* Lead Detail Modal (UPDATED with State) */}
      {selectedLead && (
        <div className="fixed inset-0 bg-black bg-opacity-50 backdrop-blur-sm flex justify-center items-center z-[2000] p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-3xl max-h-[90vh] overflow-hidden animate-fadeIn">
            {/* Modal Header */}
            <div className="bg-gradient-to-r from-blue-600 to-blue-700 px-6 py-4 flex justify-between items-center">
              <h2 className="text-xl font-bold text-white flex items-center">
                <Star className="w-5 h-5 mr-2" />
                Lead Details
              </h2>
              <button
                onClick={() => setSelectedLead(null)}
                className="text-white/80 hover:text-white transition-colors"
              >
                <X className="w-6 h-6" />
              </button>
            </div>

            {/* Modal Body */}
            <div className="p-6 overflow-y-auto max-h-[calc(90vh-120px)]">
              <div className="grid grid-cols-2 gap-6">
                {/* Left Column */}
                <div className="space-y-4">
                  <div className="bg-gray-50 p-4 rounded-xl">
                    <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Basic Information</h3>
                    <div className="space-y-3">
                      <div>
                        <label className="text-xs text-gray-500">Title</label>
                        <p className="font-medium text-gray-900">{selectedLead.title}</p>
                      </div>
                      <div>
                        <label className="text-xs text-gray-500">Category</label>
                        <p className="inline-block bg-blue-50 text-blue-700 px-3 py-1 rounded-full text-sm font-medium border border-blue-200">
                          {selectedLead.category}
                        </p>
                      </div>
                      <div>
                        <label className="text-xs text-gray-500">Location</label>
                        <p className="text-gray-900 flex items-center">
                          <MapPin className="w-4 h-4 mr-1 text-gray-500" />
                          {selectedLead.location}
                        </p>
                      </div>
                      {/* NEW: State field */}
                      <div>
                        <label className="text-xs text-gray-500">State</label>
                        <p className="text-gray-900">{selectedLead.state || 'N/A'}</p>
                      </div>
                      <div>
                        <label className="text-xs text-gray-500">Zip Code</label>
                        <p className="text-gray-900">{selectedLead.zip_code || 'N/A'}</p>
                      </div>
                    </div>
                  </div>

                  <div className="bg-gray-50 p-4 rounded-xl">
                    <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Contact Information</h3>
                    <div className="space-y-3">
                      <div>
                        <label className="text-xs text-gray-500">Phone</label>
                        <p className="text-gray-900 flex items-center">
                          <Phone className="w-4 h-4 mr-2 text-green-600" />
                          {selectedLead.phone || 'N/A'}
                        </p>
                      </div>
                      <div>
                        <label className="text-xs text-gray-500">Email</label>
                        <p className="text-gray-900 flex items-center">
                          <Mail className="w-4 h-4 mr-2 text-blue-600" />
                          {selectedLead.email || 'N/A'}
                        </p>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Right Column */}
                <div className="space-y-4">
                  <div className="bg-gray-50 p-4 rounded-xl">
                    <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Location Coordinates</h3>
                    <div className="space-y-3">
                      <div>
                        <label className="text-xs text-gray-500">Latitude</label>
                        <p className="font-mono text-gray-900">{selectedLead.latitude || 'N/A'}</p>
                      </div>
                      <div>
                        <label className="text-xs text-gray-500">Longitude</label>
                        <p className="font-mono text-gray-900">{selectedLead.longitude || 'N/A'}</p>
                      </div>
                      <div>
                        <label className="text-xs text-gray-500">Label</label>
                        <p className="text-gray-900">{selectedLead.label || 'N/A'}</p>
                      </div>
                    </div>
                  </div>

                  <div className="bg-gray-50 p-4 rounded-xl">
                    <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Status & Score</h3>
                    <div className="space-y-3">
                      <div>
                        <label className="text-xs text-gray-500">Current Status</label>
                        <p className={`inline-block px-3 py-1 rounded-full text-sm font-medium mt-1 ${getStatusColor(selectedLead.status)}`}>
                          {selectedLead.status}
                        </p>
                      </div>
                      <div>
                        <label className="text-xs text-gray-500">Score</label>
                        <div className="flex items-center mt-1">
                          <span className={`px-3 py-1 rounded-full text-xs font-semibold text-white ${getScoreColor(selectedLead.score)}`}>
                            {selectedLead.score}
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Raw Data Section */}
              <div className="mt-6">
                <div className="bg-gray-50 p-4 rounded-xl">
                  <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Raw Data</h3>
                  <pre className="bg-gray-900 text-gray-100 p-4 rounded-lg text-xs overflow-auto max-h-60 font-mono">
                    {JSON.stringify(selectedLead.raw_json, null, 2)}
                  </pre>
                </div>
              </div>
            </div>

            {/* Modal Footer */}
            <div className="bg-gray-50 px-6 py-4 border-t border-gray-200 flex justify-end">
              <button
                onClick={() => setSelectedLead(null)}
                className="bg-gradient-to-r from-gray-500 to-gray-600 hover:from-gray-600 hover:to-gray-700 text-white px-6 py-2 rounded-lg font-medium transition-all duration-200 shadow-md hover:shadow-lg"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      <style jsx>{`
        @keyframes fadeIn {
          from { opacity: 0; transform: scale(0.95); }
          to { opacity: 1; transform: scale(1); }
        }
        .animate-fadeIn { animation: fadeIn 0.2s ease-out; }
      `}</style>
    </div>
  );
};

export default Services;