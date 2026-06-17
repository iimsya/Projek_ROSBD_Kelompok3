import { useEffect, useState } from 'react'
import './App.css'

interface PredictionData {
  grid_id: str;
  prediction: number;
  status: str;
}

function App() {
  const [data, setData] = useState<PredictionData | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchPrediction = async () => {
    try {
      // Menggunakan fetch ke FastAPI Endpoint
      const response = await fetch('http://localhost:8000/api/prediction?grid_id=106_-6');
      if (response.ok) {
        const result = await response.json();
        setData(result);
      }
    } catch (error) {
      console.error("Error fetching data:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPrediction();
    // Polling setiap 30 detik untuk update peta real-time
    const interval = setInterval(fetchPrediction, 30000);
    return () => clearInterval(interval);
  }, []);

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'HIGH': return 'bg-red-500';
      case 'MEDIUM': return 'bg-yellow-500';
      case 'LOW': return 'bg-green-500';
      default: return 'bg-gray-500';
    }
  };

  return (
    <div className="min-h-screen bg-gray-900 text-white p-8 font-sans">
      <header className="mb-8">
        <h1 className="text-3xl font-bold text-blue-400">Earthquake Prediction Dashboard</h1>
        <p className="text-gray-400">Real-time Big Data Stream from USGS</p>
      </header>

      <main className="grid grid-cols-1 md:grid-cols-2 gap-8">
        {/* Map Placeholder */}
        <section className="bg-gray-800 rounded-lg p-6 flex items-center justify-center min-h-[400px] border border-gray-700">
          <div className="text-center">
            <p className="text-xl text-gray-400 mb-4">[ Interactive Map Area ]</p>
            {data && (
              <div className={`p-4 rounded-full w-24 h-24 mx-auto flex items-center justify-center shadow-lg ${getStatusColor(data.status)} animate-pulse`}>
                <span className="font-bold text-white text-lg">{data.grid_id}</span>
              </div>
            )}
          </div>
        </section>

        {/* Prediction Status */}
        <section className="bg-gray-800 rounded-lg p-6 border border-gray-700">
          <h2 className="text-2xl font-semibold mb-6">Status Wilayah Terkini</h2>
          
          {loading ? (
            <p className="text-gray-400">Memuat data dari Cassandra / API...</p>
          ) : data ? (
            <div className="space-y-4">
              <div className="flex justify-between items-center p-4 bg-gray-700 rounded">
                <span className="text-gray-300">Grid ID (Longitude_Latitude)</span>
                <span className="font-mono text-xl">{data.grid_id}</span>
              </div>
              <div className="flex justify-between items-center p-4 bg-gray-700 rounded">
                <span className="text-gray-300">Waktu Prediksi (Hari)</span>
                <span className="font-mono text-xl font-bold">{data.prediction} Hari</span>
              </div>
              <div className="flex justify-between items-center p-4 bg-gray-700 rounded">
                <span className="text-gray-300">Level Risiko</span>
                <span className={`px-4 py-1 rounded font-bold text-white ${getStatusColor(data.status)}`}>
                  {data.status} ALERT
                </span>
              </div>
            </div>
          ) : (
            <p className="text-red-400">Gagal memuat data prediksi.</p>
          )}
        </section>
      </main>
    </div>
  )
}

export default App
