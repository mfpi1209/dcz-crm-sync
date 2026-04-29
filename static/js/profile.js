// ---------------------------------------------------------------------------
// Profile (Meu Perfil) — somente leitura, alimentado por /api/me + tema local.
// ---------------------------------------------------------------------------

const ROLE_LABELS = {
    admin: 'Administrador',
    gestor: 'Gestor',
    supervisor: 'Supervisor',
    consultor: 'Consultor',
    suporte: 'Suporte',
    viewer: 'Visualizador',
};

async function loadProfile() {
    try {
        const res = await api('/api/me');
        const data = await res.json();
        _renderProfile(data);
    } catch (e) {
        console.error('loadProfile', e);
        const fallback = (document.body && document.body.getAttribute('data-username')) || '';
        _renderProfile({ username: fallback, role: 'viewer', user_id: 0 });
    }
    profSyncThemeButtons();
}

function _renderProfile(d) {
    const name = (d && d.username) || '—';
    const role = (d && d.role) || 'viewer';
    const uid = (d && d.user_id != null) ? d.user_id : '—';
    const kommo = (d && d.kommo_user_id != null && d.kommo_user_id !== '') ? d.kommo_user_id : '—';
    const cat = (d && d.categoria) ? d.categoria : '—';

    const set = (id, val) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (el.tagName === 'INPUT') el.value = val;
        else el.textContent = val;
    };

    set('prof-username', name);
    set('prof-role-label', (ROLE_LABELS[role] || role || 'Usuário').toUpperCase());
    set('prof-uid', uid);
    set('prof-kommo', kommo);
    set('prof-categoria', cat);

    set('prof-input-username', name);
    set('prof-input-role', ROLE_LABELS[role] || role || '—');
    set('prof-input-uid', String(uid));
    set('prof-input-cat', cat);

    const av = document.getElementById('prof-avatar');
    if (av) {
        const parts = String(name).trim().split(/\s+/).filter(Boolean);
        const a = parts[0] && parts[0][0] ? parts[0][0] : '';
        const b = parts.length > 1 && parts[parts.length - 1][0] ? parts[parts.length - 1][0] : '';
        av.textContent = (a + b).toUpperCase() || String(name).slice(0, 2).toUpperCase() || '–';
    }
}

function profSetTheme(theme) {
    const html = document.documentElement;
    const current = html.classList.contains('dark') ? 'dark' : 'light';
    if (current !== theme) {
        if (typeof toggleTheme === 'function') toggleTheme();
    }
    profSyncThemeButtons();
    if (typeof toast === 'function') {
        toast(theme === 'dark' ? 'Modo escuro ativado' : 'Modo claro ativado', 'success', 1800);
    }
}

function profSyncThemeButtons() {
    const isDark = document.documentElement.classList.contains('dark');
    const lightBtn = document.getElementById('prof-theme-light');
    const darkBtn = document.getElementById('prof-theme-dark');
    const activeStyle = '0 0 0 2px var(--primary) inset';
    if (lightBtn) {
        lightBtn.style.boxShadow = !isDark ? activeStyle : 'none';
        lightBtn.style.borderColor = !isDark ? 'var(--primary)' : 'var(--border)';
    }
    if (darkBtn) {
        darkBtn.style.boxShadow = isDark ? activeStyle : 'none';
        darkBtn.style.borderColor = isDark ? 'var(--primary)' : 'var(--border)';
    }
}
