import { useEffect, useState } from 'react'
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet'
import L from 'leaflet'
import { Activity, MapPin, Clock, TrendingUp, AlertTriangle } from 'lucide-react'
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts'
import './App.css'

interface RecentEvent {
  grid_id: string;
  id: string;
  time: string;
  place: string;
  magnitude: number;
  latitude: number;
  longitude: number;
  depth: number;
}

interface EventData {
  grid_id: string;
  prediction: number;
  status: string;
  place: string;
  event_time: string;
  magnitude: number;
  latitude: number;
  longitude: number;
}

interface ModelInfo {
  version: string | null;
  mae: number | null;
  rmse: number | null;
  r2: number | null;
  trained_at: string | null;
}

const createIcon = (mag: number) => {
  let cls = 'LOW';
  if (mag >= 6.0) cls = 'HIGH';
  else if (mag >= 5.0) cls = 'MEDIUM';
  return L.divIcon({
    className: 'custom-marker',
    html: `<div class="marker-dot ${cls}"></div>`,
    iconSize: [20, 20],
    iconAnchor: [10, 10]
  });
};

const createPredictionIcon = (status: string, urgent: boolean) => {
  const cls = urgent ? 'prediction urgent' : 'prediction';
  return L.divIcon({
    className: 'custom-marker',
    html: `<div class="marker-dot ${cls}" data-status="${status}">${urgent ? '!' : ''}</div>`,
    iconSize: [24, 24],
    iconAnchor: [12, 12]
  });
};

function MapUpdater({ center }: { center: [number, number] | null }) {
  const map = useMap();
  useEffect(() => {
    if (center) {
      map.flyTo(center, 6, { duration: 1.5 });
    }
  }, [center, map]);
  return null;
}

function App() {
  const [recentEvents, setRecentEvents] = useState<RecentEvent[]>([]);
  const [predictions, setPredictions] = useState<EventData[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());

  const [modelInfo, setModelInfo] = useState<ModelInfo | null>(null);

  const [filterMag, setFilterMag] = useState(false);
  const [viewMode, setViewMode] = useState<'all' | 'actual' | 'predictions'>('all');
  const [rightTab, setRightTab] = useState<'actual' | 'predictions'>('actual');
  const [mapCenter, setMapCenter] = useState<[number, number] | null>(null);

  const fetchModelInfo = async () => {
    try {
      const res = await fetch('http://localhost:8000/api/model-info');
      if (res.ok) setModelInfo(await res.json());
    } catch (e) {
      console.error("Error fetching model info:", e);
    }
  };

  const fetchData = async () => {
    try {
      const [recentRes, eventsRes] = await Promise.all([
        fetch('http://localhost:8000/api/recent?limit=100'),
        fetch('http://localhost:8000/api/events?days=30'),
      ]);
      if (recentRes.ok) {
        setRecentEvents(await recentRes.json());
      }
      if (eventsRes.ok) {
        setPredictions(await eventsRes.json());
      }
      setLastUpdate(new Date());
    } catch (error) {
      console.error("Error fetching data:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    fetchModelInfo();
    const interval = setInterval(() => {
      fetchData();
    }, 30000);
    return () => clearInterval(interval);
  }, []);

  const chartData = [...recentEvents].map(e => ({
    time: new Date(e.time + 'Z').toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}),
    magnitude: e.magnitude,
    latitude: e.latitude,
    longitude: e.longitude,
  }));

  const timeAgo = (timeStr: string) => {
    const diff = Date.now() - new Date(timeStr + 'Z').getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    return `${Math.floor(hrs / 24)}d ago`;
  };

  const getDeadlineMs = (event: EventData) => {
    if (!event.event_time) return 0;
    try {
      const utc = event.event_time.endsWith('Z') ? event.event_time : `${event.event_time}Z`;
      const eventTime = new Date(utc);
      const ms = eventTime.getTime();
      return isNaN(ms) ? 0 : ms + event.prediction * 24 * 60 * 60 * 1000;
    } catch { return 0; }
  };

  const getRemainingMs = (event: EventData) => {
    const deadline = getDeadlineMs(event);
    return deadline ? deadline - Date.now() : 0;
  };

  const getDynamicStatus = (event: EventData) => {
    const remainingMs = getRemainingMs(event);
    if (!remainingMs) return event.status;
    const remainingDays = remainingMs / (24 * 60 * 60 * 1000);
    if (remainingDays <= 1.0) return 'HIGH';
    if (remainingDays <= 3.0) return 'MEDIUM';
    return 'LOW';
  };

  const recent24h = recentEvents.filter(e => {
    return Date.now() - new Date(e.time + 'Z').getTime() < 24 * 60 * 60 * 1000;
  }).sort((a, b) => new Date(b.time).getTime() - new Date(a.time).getTime());

  const isExpired = (event: EventData) => {
    if (!event.event_time) return false;
    return Date.now() - getDeadlineMs(event) > 24 * 60 * 60 * 1000;
  };

  const within30Days = (e: EventData) => Date.now() - new Date(e.event_time + 'Z').getTime() < 30 * 24 * 60 * 60 * 1000;

  const activePredictions = predictions.filter(e => !isExpired(e) && within30Days(e))
    .sort((a, b) => getRemainingMs(a) - getRemainingMs(b));

  const formatCountdown = (event_time_str: string, prediction_days: number) => {
    try {
      const utc = event_time_str.endsWith('Z') ? event_time_str : `${event_time_str}Z`;
      const eventTime = new Date(utc);
      const targetTime = new Date(eventTime.getTime() + prediction_days * 24 * 60 * 60 * 1000);
      const diffMs = targetTime.getTime() - Date.now();
      if (diffMs <= 0) return "Kapan Saja";
      const totalSeconds = Math.floor(diffMs / 1000);
      const hours = Math.floor(totalSeconds / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      if (hours > 48) return `${Math.floor(hours / 24)} Hari ${hours % 24} Jam`;
      if (hours > 0) return `${hours} Jam ${minutes} Menit`;
      return `${minutes} Menit`;
    } catch { return `${prediction_days.toFixed(2)} hari`; }
  };

  return (
    <div className="dashboard-container">
      <header className="header">
        <h1>AI Earthquake Prediction</h1>
        <p>Real-time Global Monitoring & Aftershock Early Warning System</p>
      </header>

      <section className="stats-bar glass-panel">
        <div className="stat-card">
          <span className="stat-label">Total Grids</span>
          <span className="stat-value">{predictions.length}</span>
        </div>
        <div className="stat-divider" />
        <div className="stat-card">
          <span className="stat-label">Active</span>
          <span className="stat-value active">{predictions.filter(p => !isExpired(p) && within30Days(p)).length}</span>
        </div>
        <div className="stat-divider" />
        <div className="stat-card">
          <span className="stat-label">MAE</span>
          <span className="stat-value">{modelInfo?.mae?.toFixed(2) ?? '-'}</span>
        </div>
        <div className="stat-divider" />
        <div className="stat-card">
          <span className="stat-label">Version</span>
          <span className="stat-value">{modelInfo?.version ?? '-'}</span>
        </div>
        <div className="stat-divider" />
        <div className="stat-card">
          <span className="stat-label">Last Train</span>
          <span className="stat-value">{modelInfo?.trained_at ? new Date(modelInfo.trained_at+'Z').toLocaleDateString() : '-'}</span>
        </div>
      </section>

      <main className="main-grid">
        {/* Left Side */}
        <div className="left-column">
          <section className="map-section glass-panel">
            <div className="alerts-header" style={{ flexWrap: 'wrap', gap: '8px' }}>
              <MapPin size={24} color="#3b82f6" />
              <h2>Live Global Map</h2>
              <div style={{ marginLeft: 'auto', display: 'flex', gap: '4px', alignItems: 'center' }}>
                {(['all', 'actual', 'predictions'] as const).map(m => (
                  <button key={m} className={`toggle-btn-sm ${viewMode === m ? 'active' : ''}`} onClick={() => setViewMode(m)} style={{ textTransform: 'capitalize' }}>{m}</button>
                ))}
              </div>
              <span style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>
                Updated: {lastUpdate.toLocaleTimeString()}
              </span>
            </div>
            <div className="map-container">
              {loading ? (
                <div className="loading-container">
                  <div className="spinner"></div>
                  <p>Connecting to Cassandra...</p>
                </div>
              ) : (
                <MapContainer center={[0, 120]} zoom={3} scrollWheelZoom={true} style={{ height: '100%', width: '100%' }}>
                  <TileLayer
                    attribution='&copy; OpenStreetMap'
                    url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
                  />
                  <MapUpdater center={mapCenter} />
                  {(viewMode === 'all' || viewMode === 'actual') && recentEvents.filter(e => Date.now() - new Date(e.time + 'Z').getTime() < 24 * 60 * 60 * 1000).map((event) => (
                    <Marker
                      key={event.id || event.grid_id}
                      position={[event.latitude, event.longitude]}
                      icon={createIcon(event.magnitude)}
                    >
                      <Popup className="custom-popup">
                        <strong>{event.place}</strong><br/>
                        Mag: {event.magnitude.toFixed(1)}<br/>
                        Depth: {event.depth.toFixed(1)} km<br/>
                        Time: {new Date(event.time + 'Z').toLocaleString()}
                      </Popup>
                    </Marker>
                  ))}
                  {(viewMode === 'all' || viewMode === 'predictions') && predictions.filter(p => p.latitude && p.longitude && within30Days(p) && getRemainingMs(p) <= 48 * 60 * 60 * 1000 && getRemainingMs(p) > 0).map((p) => {
                    const dynStatus = getDynamicStatus(p);
                    return (
                      <Marker
                        key={`pred-${p.grid_id}`}
                        position={[p.latitude, p.longitude]}
                        icon={createPredictionIcon(dynStatus, true)}
                      >
                        <Popup className="custom-popup">
                          <strong>{p.place}</strong><br/>
                          Predicted Mag: M{p.magnitude.toFixed(1)}<br/>
                          Status: {dynStatus}<br/>
                          <span style={{color:'#ef4444'}}>⚠ Immediate Risk</span><br/>
                          Event: {new Date(p.event_time + 'Z').toLocaleString()}
                        </Popup>
                      </Marker>
                    );
                  })}
                </MapContainer>
              )}
            </div>
          </section>

          {/* Charts Row */}
          <div className="charts-row">
            <section className="analytics-section glass-panel">
              <div className="alerts-header">
                <TrendingUp size={24} color="#10b981" />
                <h2>Magnitude Trend (Recent)</h2>
              </div>
              <div style={{ width: '100%', height: '160px' }}>
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart
                    data={chartData}
                    onClick={(e) => {
                      if (e && e.activePayload && e.activePayload.length > 0) {
                        setMapCenter([e.activePayload[0].payload.latitude, e.activePayload[0].payload.longitude]);
                      }
                    }}
                    style={{ cursor: 'pointer' }}
                  >
                    <defs>
                      <linearGradient id="colorMag" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.8}/>
                        <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(100,116,139,0.1)" />
                    <XAxis dataKey="time" stroke="#64748b" fontSize={12} tickLine={false} />
                    <YAxis stroke="#64748b" fontSize={12} tickLine={false} domain={['auto', 'auto']} />
                    <Tooltip
                      contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.9)', border: '1px solid rgba(100,116,139,0.1)', borderRadius: '8px' }}
                      itemStyle={{ color: '#334155' }}
                    />
                    <Area
                      type="monotone" dataKey="magnitude" stroke="#3b82f6" fillOpacity={1} fill="url(#colorMag)"
                      activeDot={{ onClick: (e, payload) => { if (payload?.payload) setMapCenter([payload.payload.latitude, payload.payload.longitude]); }, cursor: 'pointer', r: 6 }}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </section>
          </div>
        </div>

        {/* Right Side: Tab Panel */}
        <section className="alerts-section glass-panel" style={{ height: '802px', display: 'flex', flexDirection: 'column' }}>
          <div className="tab-bar">
            <button className={`tab-btn ${rightTab === 'actual' ? 'active' : ''}`} onClick={() => setRightTab('actual')}>Actual</button>
            <button className={`tab-btn ${rightTab === 'predictions' ? 'active' : ''}`} onClick={() => setRightTab('predictions')}>Prediksi</button>
          </div>

          {rightTab === 'actual' && (
            <>
              <div className="alerts-header" style={{ flexWrap: 'wrap', gap: '8px', marginBottom: '8px', marginTop: '12px' }}>
                <Activity size={20} color="#3b82f6" />
                <h2 style={{ fontSize: '1.1rem' }}>Recent 24h</h2>
                <span style={{marginLeft: 'auto', fontSize: '0.8rem', color: 'var(--text-tertiary)'}}>
                  {recent24h.length} events
                </span>
              </div>
              <div className="recent-compact" style={{ marginBottom: '8px' }}>
                {recent24h.slice(0, 10).map(e => {
                  const c = e.magnitude >= 6.0 ? '#c05a5a' : e.magnitude >= 5.0 ? '#c58b43' : '#599c7a';
                  return (
                    <div key={e.id} className="recent-row" onClick={() => setMapCenter([e.latitude, e.longitude])}>
                      <span className="recent-mag" style={{ color: c }}>M{e.magnitude.toFixed(1)}</span>
                      <span className="recent-place">{e.place}</span>
                      <span className="recent-time">{timeAgo(e.time)}</span>
                    </div>
                  );
                })}
                {recent24h.length === 0 && <div className="recent-row" style={{ color: 'var(--text-tertiary)' }}>No events in last 24h</div>}
              </div>
            </>
          )}

          {rightTab === 'predictions' && (
            <>
              <div className="alerts-header" style={{ flexWrap: 'wrap', gap: '8px', marginBottom: '8px', marginTop: '12px' }}>
                <AlertTriangle size={20} color="#ef4444" />
                <h2 style={{ fontSize: '1.1rem' }}>Active Predictions</h2>
                <div style={{ marginLeft: 'auto', display: 'flex', gap: '6px', alignItems: 'center' }}>
                  <button className={`filter-btn-sm ${!filterMag ? 'active' : ''}`} onClick={() => setFilterMag(false)}>All</button>
                  <button className={`filter-btn-sm ${filterMag ? 'active' : ''}`} onClick={() => setFilterMag(true)}>≥ M5.0</button>
                </div>
              </div>
              {loading ? (
                <div className="loading-container"><div className="spinner"></div></div>
              ) : predictions.length === 0 ? (
                <div className="loading-container"><p>Belum ada data prediksi.</p></div>
              ) : (
                <div className="alerts-list">
                  {activePredictions.filter(e => filterMag ? e.magnitude >= 5.0 : true).map((event) => {
                    const dynStatus = getDynamicStatus(event);
                    const remainingMs = getRemainingMs(event);
                    const isUrgent = remainingMs <= 48 * 60 * 60 * 1000;
                    return (
                      <div key={event.grid_id} className={`alert-item status-${dynStatus}`} onClick={() => setMapCenter([event.latitude, event.longitude])} style={{ cursor: 'pointer', padding: '14px 16px' }}>
                        <div className="alert-header" style={{ marginBottom: '6px' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0 }}>
                            <span className="alert-mag" style={{ flexShrink: 0 }}>M{event.magnitude.toFixed(1)}</span>
                            <h3 className="alert-place" style={{ fontSize: '0.95rem', margin: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{event.place}</h3>
                          </div>
                          <span className={`alert-badge status-${dynStatus}`}>{dynStatus}</span>
                        </div>
                        <div className="alert-body" style={{ gap: '4px' }}>
                          <div className="alert-stat"><Clock size={14} /><span style={{ fontSize: '0.85rem' }}>Prediksi susulan:</span></div>
                          <div className={`countdown ${isUrgent ? 'urgent' : ''}`} style={{ fontSize: '1.1rem' }}>{formatCountdown(event.event_time, event.prediction)}</div>
                          {isUrgent && (
                            <div className="alert-stat" style={{ color: '#fca5a5', marginTop: '2px' }}>
                              <AlertTriangle size={12} /><span style={{ fontSize: '0.75rem' }}>Bahaya Tinggi dalam 48 Jam</span>
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })}
                  {activePredictions.length === 0 && <div className="loading-container"><p>Tidak ada prediksi aktif.</p></div>}
                </div>
              )}
            </>
          )}
        </section>
      </main>
    </div>
  )
}

export default App
