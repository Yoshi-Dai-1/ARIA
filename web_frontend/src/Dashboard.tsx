import React from 'react';

const Dashboard: React.FC = () => {
  return (
    <div className="p-8 bg-gray-900 min-h-screen text-white font-sans">
      <header className="mb-12">
        <h1 className="text-4xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-emerald-400">
          ARIA Investment Dashboard
        </h1>
        <p className="text-gray-400 mt-2">日本株投資データ統合プラットフォーム</p>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* ステータスカード */}
        <div className="bg-gray-800 p-6 rounded-xl border border-gray-700 hover:border-blue-500 transition-all shadow-lg">
          <h3 className="text-sm font-semibold text-blue-400 uppercase tracking-wider">Data Engine Status</h3>
          <p className="text-2xl font-bold mt-2">Active</p>
          <p className="text-gray-500 text-xs mt-1">Last sync: 今日 03:00 (GHA)</p>
        </div>

        <div className="bg-gray-800 p-6 rounded-xl border border-gray-700 hover:border-emerald-500 transition-all shadow-lg">
          <h3 className="text-sm font-semibold text-emerald-400 uppercase tracking-wider">Hugging Face Storage</h3>
          <p className="text-2xl font-bold mt-2">Connected</p>
          <p className="text-gray-500 text-xs mt-1">Dataset: yoshi_dai/ARIA-Data</p>
        </div>

        <div className="bg-gray-800 p-6 rounded-xl border border-gray-700 hover:border-purple-500 transition-all shadow-lg">
          <h3 className="text-sm font-semibold text-purple-400 uppercase tracking-wider">Market Coverage</h3>
          <p className="text-2xl font-bold mt-2">3,800+ Stocks</p>
          <p className="text-gray-500 text-xs mt-1">Normalizing to 5-digit codes...</p>
        </div>
      </div>

      <main className="mt-12 bg-gray-800 rounded-2xl p-8 border border-gray-700">
        <h2 className="text-2xl font-semibold mb-6">プロジェクト・ビジョン</h2>
        <div className="space-y-4">
          <div className="bg-gray-900 p-4 rounded-lg flex items-center">
            <span className="w-8 h-8 rounded-full bg-blue-500 flex items-center justify-center mr-4 text-xs">01</span>
            <p>完全無料で永久に動作し続けるインフラ構成（GHA + HF）</p>
          </div>
          <div className="bg-gray-900 p-4 rounded-lg flex items-center">
            <span className="w-8 h-8 rounded-full bg-emerald-500 flex items-center justify-center mr-4 text-xs">02</span>
            <p>5桁証券コード正規化による優先株・子会社の完全網羅</p>
          </div>
          <div className="bg-gray-900 p-4 rounded-lg flex items-center">
            <span className="w-8 h-8 rounded-full bg-purple-500 flex items-center justify-center mr-4 text-xs">03</span>
            <p>AI 自律保守 (Skill Architect) による持続可能な開発</p>
          </div>
        </div>
      </main>

      <footer className="mt-12 text-center text-gray-500 text-sm">
        &copy; 2026 ARIA Project - Built with Antigravity & AI Autonomy
      </footer>
    </div>
  );
};

export default Dashboard;
