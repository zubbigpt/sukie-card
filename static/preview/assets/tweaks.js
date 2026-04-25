/* Tweaks panel — palette + type toggles shared across all pages */
(function () {
  const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
    "palette": "cream",
    "type": "serif-geist",
    "dark": false
  }/*EDITMODE-END*/;

  // Load from localStorage first (so tweaks persist across navigation between pages)
  const stored = (() => {
    try { return JSON.parse(localStorage.getItem('zub_tweaks') || '{}'); }
    catch { return {}; }
  })();
  const state = Object.assign({}, TWEAK_DEFAULTS, stored);

  function apply() {
    const pal = state.dark ? 'ink' : state.palette;
    document.documentElement.setAttribute('data-palette', pal);
    document.documentElement.setAttribute('data-type', state.type);
  }
  apply();

  function persist() {
    try { localStorage.setItem('zub_tweaks', JSON.stringify(state)); } catch {}
  }

  let panel = null;
  function buildPanel() {
    if (panel) return panel;
    panel = document.createElement('div');
    panel.id = 'zub-tweaks';
    panel.innerHTML = `
      <style>
        #zub-tweaks {
          position: fixed; right: 16px; bottom: 16px; z-index: 9999;
          background: var(--surface); color: var(--ink);
          border: 1px solid var(--line); border-radius: 14px;
          padding: 14px; width: 280px;
          box-shadow: 0 20px 60px rgba(0,0,0,.15);
          font-family: var(--font-ui);
          display: none;
        }
        #zub-tweaks.open { display: block; }
        #zub-tweaks h4 {
          font-family: var(--font-mono); font-size: .68rem; letter-spacing: .14em;
          text-transform: uppercase; color: var(--ink-3);
          margin: 0 0 8px 0; font-weight: 600;
        }
        #zub-tweaks .row { display: grid; grid-template-columns: repeat(4,1fr); gap: 6px; margin-bottom: 12px; }
        #zub-tweaks .row.two { grid-template-columns: repeat(2,1fr); }
        #zub-tweaks button.opt {
          border: 1px solid var(--line); background: var(--bg);
          padding: 8px 4px; border-radius: 8px;
          font-family: var(--font-mono); font-size: .66rem; letter-spacing: .08em;
          text-transform: uppercase; color: var(--ink-2); cursor: pointer;
          transition: all .15s var(--ease);
        }
        #zub-tweaks button.opt:hover { border-color: var(--ink); }
        #zub-tweaks button.opt.active { background: var(--ink); color: var(--bg); border-color: var(--ink); }
        #zub-tweaks .swatch { display: flex; align-items: center; gap: 4px; justify-content: center; }
        #zub-tweaks .swatch .dot { width: 10px; height: 10px; border-radius: 50%; border: 1px solid rgba(0,0,0,.1); }
        #zub-tweaks .close-x {
          position: absolute; top: 8px; right: 8px; background: none; border: none;
          cursor: pointer; font-size: 16px; color: var(--ink-3); line-height: 1;
        }
      </style>
      <button class="close-x" data-close>×</button>
      <h4>Palette</h4>
      <div class="row" data-group="palette">
        <button class="opt" data-val="cream"><span class="swatch"><span class="dot" style="background:#f4ede2"></span><span class="dot" style="background:oklch(0.64 0.15 55)"></span></span></button>
        <button class="opt" data-val="espresso"><span class="swatch"><span class="dot" style="background:#1a120b"></span><span class="dot" style="background:oklch(0.78 0.14 82)"></span></span></button>
        <button class="opt" data-val="mint"><span class="swatch"><span class="dot" style="background:#f6f5f0"></span><span class="dot" style="background:oklch(0.68 0.11 155)"></span></span></button>
        <button class="opt" data-val="ink"><span class="swatch"><span class="dot" style="background:#14110d"></span><span class="dot" style="background:oklch(0.78 0.14 82)"></span></span></button>
      </div>
      <h4>Typography</h4>
      <div class="row two" data-group="type">
        <button class="opt" data-val="serif-geist">Instrument · Geist</button>
        <button class="opt" data-val="fraunces-inter">Fraunces · Inter</button>
        <button class="opt" data-val="epilogue-manrope">Epilogue · Manrope</button>
        <button class="opt" data-val="bricolage-dm">Bricolage · DM</button>
      </div>
    `;
    document.body.appendChild(panel);

    panel.querySelectorAll('[data-group]').forEach(group => {
      const key = group.getAttribute('data-group');
      group.querySelectorAll('button.opt').forEach(btn => {
        if (btn.getAttribute('data-val') === state[key] || (key==='palette' && state.dark && btn.getAttribute('data-val')==='ink')) {
          btn.classList.add('active');
        }
        btn.addEventListener('click', () => {
          const v = btn.getAttribute('data-val');
          if (key === 'palette') {
            state.dark = (v === 'ink');
            state.palette = (v === 'ink') ? state.palette : v;
          } else {
            state[key] = v;
          }
          apply();
          persist();
          try { window.parent.postMessage({type: '__edit_mode_set_keys', edits: state}, '*'); } catch {}
          group.querySelectorAll('button.opt').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
        });
      });
    });
    panel.querySelector('[data-close]').addEventListener('click', () => {
      panel.classList.remove('open');
    });
    return panel;
  }

  // Listen for host activate/deactivate
  window.addEventListener('message', (e) => {
    if (!e.data || typeof e.data !== 'object') return;
    if (e.data.type === '__activate_edit_mode') {
      buildPanel().classList.add('open');
    } else if (e.data.type === '__deactivate_edit_mode') {
      if (panel) panel.classList.remove('open');
    }
  });
  try { window.parent.postMessage({type: '__edit_mode_available'}, '*'); } catch {}

  // Expose for in-page toggles
  window.ZubTweaks = {
    toggle() { buildPanel().classList.toggle('open'); },
    state
  };
})();
