import { useEffect, useState, useRef } from 'react'
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet'
import L from 'leaflet'
import { Activity, MapPin, AlertTriangle, Clock, Volume2, VolumeX, Filter, TrendingUp } from 'lucide-react'
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts'
import './App.css'

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

// Custom Marker Icons for Leaflet
const createIcon = (status: string, isTsunamiRisk: boolean) => {
  return L.divIcon({
    className: 'custom-marker',
    html: `<div class="marker-dot ${status} ${isTsunamiRisk ? 'tsunami-radar' : ''}"></div>`,
    iconSize: [20, 20],
    iconAnchor: [10, 10]
  });
};

// Component untuk menggeser peta secara animasi
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
  const [events, setEvents] = useState<EventData[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());
  
  // New States for Features
  const [soundEnabled, setSoundEnabled] = useState(true);
  const [filter, setFilter] = useState<'ALL' | 'HIGH' | 'MEDIUM' | 'LOW' | 'MAGNITUDE'>('ALL');
  const [mapCenter, setMapCenter] = useState<[number, number] | null>(null);
  
  // Audio Ref
  const audioRef = useRef<HTMLAudioElement | null>(null);

  useEffect(() => {
    // Sound effect URL
    audioRef.current = new Audio('https://assets.mixkit.co/active_storage/sfx/2869/2869-preview.mp3'); 
  }, []);

  const fetchEvents = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/events');
      if (response.ok) {
        const result: EventData[] = await response.json();
        
        setEvents(result);
        setLastUpdate(new Date());
        
        // Trigger sound if conditions met
        if (soundEnabled && audioRef.current) {
          const hasUrgentHigh = result.some(e => e.status === 'HIGH' && e.prediction <= 1.0); // Less than 24 hours
          if (hasUrgentHigh) {
            audioRef.current.play().catch(e => console.log("Audio play blocked", e));
          }
        }
      }
    } catch (error) {
      console.error("Error fetching data:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchEvents();
    const interval = setInterval(fetchEvents, 30000);
    return () => clearInterval(interval);
  }, [soundEnabled]); 

  const formatCountdown = (event_time_str: string, prediction_days: number) => {
    try {
      const utcTimeString = event_time_str.endsWith('Z') ? event_time_str : `${event_time_str}Z`;
      const eventTime = new Date(utcTimeString);
      const targetTime = new Date(eventTime.getTime() + prediction_days * 24 * 60 * 60 * 1000);
      const now = new Date();
      const diffMs = targetTime.getTime() - now.getTime();
      
      if (diffMs <= 0) return "🚨 WAKTU TERLEWATI!";
      
      const totalSeconds = Math.floor(diffMs / 1000);
      const hours = Math.floor(totalSeconds / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      return `${hours} Jam ${minutes} Menit lagi`;
    } catch (e) {
      return `${prediction_days.toFixed(2)} hari`;
    }
  };

  // Process data for charts
  const chartData = [...events]
    .sort((a, b) => new Date(a.event_time).getTime() - new Date(b.event_time).getTime())
    .map(e => ({
      time: new Date(e.event_time.endsWith('Z') ? e.event_time : e.event_time+'Z').toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}),
      magnitude: e.magnitude,
      latitude: e.latitude,
      longitude: e.longitude
    }));

  // Filtered Events
  const filteredEvents = [...events].filter(e => {
    if (filter === 'HIGH') return e.status === 'HIGH';
    if (filter === 'MEDIUM') return e.status === 'MEDIUM';
    if (filter === 'LOW') return e.status === 'LOW';
    if (filter === 'MAGNITUDE') return e.magnitude >= 5.0;
    return true;
  }).sort((a, b) => {
    if (filter === 'MAGNITUDE') return b.magnitude - a.magnitude;
    return a.prediction - b.prediction; // Default sort by urgency
  });

  return (
    <div className="dashboard-container">
      <header className="header">
        <h1>AI Earthquake Prediction</h1>
        <p>Real-time Global Monitoring & Aftershock Early Warning System</p>
      </header>

      <main className="main-grid">
        {/* Left Side: Interactive Map & Analytics */}
        <div className="left-column">
          <section className="map-section glass-panel">
            <div className="alerts-header">
              <MapPin size={24} color="#3b82f6" />
              <h2>Live Global Map</h2>
              <span style={{marginLeft: 'auto', fontSize: '0.85rem', color: 'var(--text-secondary)'}}>
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
                  {events.map((event) => {
                    // Check if it's potentially oceanic (a simplification for visual tsunami radar)
                    const isTsunamiRisk = event.magnitude >= 6.0;
                    return (
                      <Marker 
                        key={event.grid_id} 
                        position={[event.latitude, event.longitude]}
                        icon={createIcon(event.status, isTsunamiRisk)}
                      >
                        <Popup className="custom-popup">
                          <strong>{event.place}</strong><br/>
                          Mag: {event.magnitude.toFixed(1)}<br/>
                          Status: {event.status}<br/>
                          Prediksi: {formatCountdown(event.event_time, event.prediction)}
                        </Popup>
                      </Marker>
                    );
                  })}
                </MapContainer>
              )}
            </div>
          </section>

          {/* Analytics Section */}
          <section className="analytics-section glass-panel" style={{ height: '250px' }}>
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
                      const data = e.activePayload[0].payload;
                      setMapCenter([data.latitude, data.longitude]);
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
                    type="monotone" 
                    dataKey="magnitude" 
                    stroke="#3b82f6" 
                    fillOpacity={1} 
                    fill="url(#colorMag)" 
                    activeDot={{ 
                      onClick: (e, payload) => {
                        if (payload && payload.payload) {
                          const data = payload.payload;
                          setMapCenter([data.latitude, data.longitude]);
                        }
                      },
                      cursor: 'pointer',
                      r: 6
                    }}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </section>
        </div>

        {/* Right Side: Active Alerts List */}
        <section className="alerts-section glass-panel" style={{ height: '802px', display: 'flex', flexDirection: 'column' }}>
          <div className="alerts-header" style={{ flexWrap: 'wrap', gap: '10px' }}>
            <Activity size={24} color="#ef4444" />
            <h2>Active Threat Board</h2>
            
            {/* Audio Toggle */}
            <button 
              className={`icon-btn ${soundEnabled ? 'active' : ''}`}
              onClick={() => setSoundEnabled(!soundEnabled)}
              title="Toggle Audio Siren (For High < 3 Hours)"
            >
              {soundEnabled ? <Volume2 size={18} /> : <VolumeX size={18} />}
            </button>
            
            <span style={{marginLeft: 'auto', background: 'rgba(59, 130, 246, 0.2)', padding: '2px 8px', borderRadius: '12px', fontSize: '0.85rem'}}>
              {filteredEvents.length} Terpantau
            </span>
          </div>

          {/* Filters */}
          <div className="filters-container">
            <Filter size={16} color="#94a3b8" />
            <button className={`filter-btn ${filter === 'ALL' ? 'active' : ''}`} onClick={() => setFilter('ALL')}>All</button>
            <button className={`filter-btn ${filter === 'HIGH' ? 'active' : ''}`} onClick={() => setFilter('HIGH')}>High</button>
            <button className={`filter-btn ${filter === 'MEDIUM' ? 'active' : ''}`} onClick={() => setFilter('MEDIUM')}>Medium</button>
            <button className={`filter-btn ${filter === 'LOW' ? 'active' : ''}`} onClick={() => setFilter('LOW')}>Low</button>
            <button className={`filter-btn ${filter === 'MAGNITUDE' ? 'active' : ''}`} onClick={() => setFilter('MAGNITUDE')}>&ge; M 5.0</button>
          </div>

          {loading ? (
            <div className="loading-container">
              <div className="spinner"></div>
            </div>
          ) : filteredEvents.length === 0 ? (
            <div className="loading-container">
              <p>Tidak ada ancaman terdeteksi.</p>
            </div>
          ) : (
            <div className="alerts-list">
              {filteredEvents.map((event) => {
                const isUrgent = event.prediction <= 1.0;
                
                return (
                  <div 
                    key={event.grid_id} 
                    className={`alert-item status-${event.status}`}
                    onClick={() => setMapCenter([event.latitude, event.longitude])}
                    style={{ cursor: 'pointer' }}
                  >
                    <div className="alert-header">
                      <div>
                        <h3 className="alert-place">{event.place}</h3>
                        <span className="alert-mag">Mag {event.magnitude.toFixed(1)}</span>
                      </div>
                      <span className={`alert-badge status-${event.status}`}>
                        {event.status}
                      </span>
                    </div>
                    
                    <div className="alert-body">
                      <div className="alert-stat">
                        <Clock size={16} />
                        <span>Estimasi Susulan ({'>='} M 4.0):</span>
                      </div>
                      <div className={`countdown ${isUrgent ? 'urgent' : ''}`}>
                        {formatCountdown(event.event_time, event.prediction)}
                      </div>
                      
                      {isUrgent && (
                        <div className="alert-stat" style={{ color: '#fca5a5', marginTop: '4px' }}>
                          <AlertTriangle size={14} />
                          <span style={{ fontSize: '0.8rem' }}>Bahaya Tinggi dalam 24 Jam</span>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </section>
      </main>
    </div>
  )
}

export default App
