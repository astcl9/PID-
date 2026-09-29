(function (root) {
  const typeRank = new Map([['PV', 0], ['SV', 1], ['MV', 2], ['OTHER', 3]]);

  function signalLoop(tag) {
    const value = String(tag || '');
    const dot = value.lastIndexOf('.');
    return dot > 0 ? value.slice(0, dot) : value;
  }

  function listLoops(signals, declaredLoops) {
    const source = Array.isArray(declaredLoops) && declaredLoops.length
      ? declaredLoops
      : (signals || []).map(signal => signalLoop(signal.tag));
    return [...new Set(source.map(String).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b, 'zh-CN', { numeric: true }));
  }

  function defaultTagsByLoop(signals, options) {
    const opts = options || {};
    const allowed = new Set((opts.types || ['PV', 'SV', 'MV', 'OTHER']).map(type => String(type).toUpperCase()));
    const activeLoop = String(opts.loop || '');
    const activeLoops = new Set((opts.loops || []).map(String).filter(Boolean));
    const cap = Math.max(0, Number(opts.cap ?? 12));
    const matches = (signals || []).filter(signal => {
      const type = String(signal.type || 'OTHER').toUpperCase();
      return allowed.has(type) && ((!activeLoop && !activeLoops.size) || signalLoop(signal.tag) === activeLoop || activeLoops.has(signalLoop(signal.tag)));
    });
    if (activeLoop || activeLoops.size) return matches.map(signal => signal.tag);

    const representatives = new Map();
    for (const signal of matches) {
      const loop = signalLoop(signal.tag);
      if (!loop) continue;
      const current = representatives.get(loop);
      const rank = typeRank.get(String(signal.type || 'OTHER').toUpperCase()) ?? typeRank.get('OTHER');
      const currentRank = current
        ? (typeRank.get(String(current.type || 'OTHER').toUpperCase()) ?? typeRank.get('OTHER'))
        : Infinity;
      if (!current || rank < currentRank) representatives.set(loop, signal);
    }
    return [...representatives.entries()]
      .sort(([a], [b]) => a.localeCompare(b, 'zh-CN', { numeric: true }))
      .slice(0, cap)
      .map(([, signal]) => signal.tag);
  }

  root.PidLoopSelection = { signalLoop, listLoops, defaultTagsByLoop };
})(window);
