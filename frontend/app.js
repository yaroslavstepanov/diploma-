// Справочники — загружаются с API при старте (fallback при ошибке)
let DATA_TYPE_GENERATORS = {};
let GENERATOR_PARAMS = {};
let GENERATOR_LABELS = {};
let ALL_GENERATORS = [];
let SUPPORTED_TYPES = [];
let DICTIONARIES = [];
let REFERENCE_READY = false;

// Fallback при недоступности API
const FALLBACK_DATA_TYPE_GENERATORS = {
    'String': { random: ['random_digits', 'uuid4', 'url_template', 'enum_choice', 'regex'], sequential: [], fixed: ['enum_choice'] },
    'Int32': { random: ['random_int'], sequential: ['sequence_int'], fixed: ['enum_choice'] },
    'DateTime': { random: ['timestamp_asc', 'timestamp_desc'], sequential: ['timestamp_asc', 'timestamp_desc'], fixed: ['enum_choice'] },
    'UUID': { random: ['uuid4'], sequential: [], fixed: [] }
};

const FALLBACK_GENERATOR_PARAMS = {
    'random_int': { min: { type: 'number', label: 'Минимум', default: 0 }, max: { type: 'number', label: 'Максимум', default: 100 }, use_float: { type: 'checkbox', label: 'Генерировать вещественные числа', default: false }, precision: { type: 'number', label: 'Знаков после запятой', default: 2, min: 0, max: 10 } },
    'sequence_int': { start: { type: 'number', label: 'Начальное значение', default: 0 }, step: { type: 'number', label: 'Шаг', default: 1 }, probability: { type: 'number', label: 'Вероятность (%)', default: 100, min: 0, max: 100, step: 0.01, placeholder: '75.25' } },
    'timestamp_asc': { start: { type: 'text', label: 'Начало', placeholder: 'now или ISO-8601', default: 'now' }, step: { type: 'text', label: 'Шаг', placeholder: '1s, 5m, 2h, 1d', default: '1s' } },
    'timestamp_desc': { start: { type: 'text', label: 'Начало', placeholder: 'now или ISO-8601', default: 'now' }, step: { type: 'text', label: 'Шаг', placeholder: '1s, 5m, 2h, 1d', default: '1s' } },
    'random_digits': { length: { type: 'number', label: 'Длина', default: 8, min: 1, max: 100 } },
    'uuid4': {},
    'url_template': { pattern: { type: 'text', label: 'Шаблон', placeholder: 'https://example.com/item/{row}?uuid={uuid}', default: 'https://example.com/item/{row}?uuid={uuid}' } },
    'enum_choice': {
        source: { type: 'select', label: 'Источник', default: 'inline', options: ['inline', 'dictionary'], optionLabels: { 'inline': 'Ввести вручную', 'dictionary': 'Из словаря' } },
        dictionary: { type: 'dictionary_select', label: 'Словарь', default: '' },
        mode: { type: 'select', label: 'Режим', default: 'random', options: ['random', 'sequential'], optionLabels: { 'random': 'Случайный', 'sequential': 'По очереди' } },
        values: { type: 'textarea', label: 'Значения (по строке)', placeholder: 'value1\nvalue2' },
        weights: { type: 'textarea', label: 'Вероятности % (опционально)', placeholder: '50\n30\n20' }
    },
    'regex': {
        preset: { type: 'select', label: 'Формат', default: '', options: ['', 'ru_passport', 'ru_phone', 'mac_address'], optionLabels: { '': 'Свой regex', 'ru_passport': 'Паспорт РФ', 'ru_phone': 'Телефон РФ (+7)', 'mac_address': 'MAC-адрес' } },
        pattern: { type: 'text', label: 'Регулярное выражение', placeholder: '[A-Z]{3}-\\d{4}', default: '[a-z0-9]{8}' }
    }
};

const FALLBACK_GENERATOR_LABELS = {
    'random_int': 'Случайные числа', 'sequence_int': 'Последовательность', 'timestamp_asc': 'Даты по возрастанию',
    'timestamp_desc': 'Даты по убыванию', 'random_digits': 'Случайные цифры', 'uuid4': 'UUID',
    'url_template': 'URL шаблон', 'enum_choice': 'Список значений', 'regex': 'По regex'
};
const FALLBACK_ALL_GENERATORS = [
    { kind: 'uuid4', label: 'UUID', defaultType: 'UUID' },
    { kind: 'random_int', label: 'Случайные числа', defaultType: 'Int32' },
    { kind: 'sequence_int', label: 'Последовательность', defaultType: 'Int32' },
    { kind: 'random_digits', label: 'Случайные цифры', defaultType: 'String' },
    { kind: 'url_template', label: 'URL шаблон', defaultType: 'String' },
    { kind: 'regex', label: 'По regex', defaultType: 'String' },
    { kind: 'timestamp_asc', label: 'Даты по возрастанию', defaultType: 'DateTime' },
    { kind: 'timestamp_desc', label: 'Даты по убыванию', defaultType: 'DateTime' },
    { kind: 'enum_choice', label: 'Список значений', defaultType: 'String' }
];

const FALLBACK_SUPPORTED_TYPES = [
    { id: 'String', label: 'Строка' },
    { id: 'Int32', label: 'Число' },
    { id: 'DateTime', label: 'Дата' },
    { id: 'UUID', label: 'UUID' }
];

const SEQUENTIAL_KINDS = ['sequence_int', 'timestamp_asc', 'timestamp_desc'];
const FIXED_KINDS = ['enum_choice'];

function buildFromApi(generators, types) {
    const dtypeGen = {};
    const genParams = {};
    const genLabels = {};

    for (const t of types) {
        dtypeGen[t.id] = { random: [], sequential: [], fixed: [] };
        for (const g of generators) {
            if (!g.compatible_types || !g.compatible_types.includes(t.id)) continue;
            if (FIXED_KINDS.includes(g.kind)) dtypeGen[t.id].fixed.push(g.kind);
            if (SEQUENTIAL_KINDS.includes(g.kind)) dtypeGen[t.id].sequential.push(g.kind);
            if (FIXED_KINDS.includes(g.kind) || !SEQUENTIAL_KINDS.includes(g.kind)) {
                dtypeGen[t.id].random.push(g.kind);
            }
        }
        dtypeGen[t.id].random = [...new Set(dtypeGen[t.id].random)];
        dtypeGen[t.id].sequential = [...new Set(dtypeGen[t.id].sequential)];
    }

    const allGens = [];
    for (const g of generators) {
        const label = g.description ? g.description.split('.')[0].trim() : g.kind;
        genLabels[g.kind] = label;
        const defaultType = (g.compatible_types && g.compatible_types[0]) || 'String';
        allGens.push({ kind: g.kind, label, defaultType });
        const params = {};
        for (const p of g.params || []) {
            let fType = p.type;
            if (fType === 'string') fType = 'text';
            if (fType === 'boolean') fType = 'checkbox';
            if (fType === 'array') fType = 'textarea';
            const cfg = {
                type: fType,
                label: p.description || p.name,
                default: p.default
            };
            if (p.placeholder) cfg.placeholder = p.placeholder;
            if (p.min != null) cfg.min = p.min;
            if (p.max != null) cfg.max = p.max;
            if (p.options) cfg.options = p.options;
            if (p.option_labels) cfg.optionLabels = p.option_labels;
            if (p.name === 'dictionary' && g.kind === 'enum_choice') cfg.type = 'dictionary_select';
            if (p.name === 'probability') {
                cfg.label = 'Вероятность последовательного значения (%)';
                cfg.default = p.default != null ? p.default * 100 : 100;
                cfg.min = 0;
                cfg.max = 100;
                cfg.step = 0.01;
                cfg.placeholder = '75.25';
            }
            params[p.name] = cfg;
        }
        genParams[g.kind] = params;
    }
    return { dtypeGen, genParams, genLabels, allGenerators: allGens };
}

async function loadReferenceData() {
    const ts = Date.now();
    try {
        const [genRes, typesRes, dictRes] = await Promise.all([
            fetch(`/api/generators?_=${ts}`, { cache: 'no-store' }),
            fetch(`/api/supported-types?_=${ts}`, { cache: 'no-store' }),
            fetch(`/api/dictionaries?_=${ts}`, { cache: 'no-store' }).catch(() => ({ ok: false }))
        ]);
        if (!genRes.ok || !typesRes.ok) throw new Error('API error');
        const genJson = await genRes.json();
        const typesJson = await typesRes.json();
        const generators = genJson.generators || [];
        const types = typesJson.types || [];
        if (!generators.length || !types.length) throw new Error('Empty data');
        DICTIONARIES = dictRes.ok ? ((await dictRes.json()).dictionaries || []).map(d => ({ value: d.name, label: `${d.name} (${d.values_count})` })) : [];
        const { dtypeGen, genParams, genLabels, allGenerators } = buildFromApi(generators, types);
        DATA_TYPE_GENERATORS = dtypeGen;
        GENERATOR_PARAMS = genParams;
        GENERATOR_LABELS = genLabels;
        ALL_GENERATORS = allGenerators;
        SUPPORTED_TYPES = types;
        REFERENCE_READY = true;
        return true;
    } catch (e) {
        console.warn('Reference API недоступен, используем fallback:', e.message);
        DATA_TYPE_GENERATORS = FALLBACK_DATA_TYPE_GENERATORS;
        GENERATOR_PARAMS = FALLBACK_GENERATOR_PARAMS;
        GENERATOR_LABELS = FALLBACK_GENERATOR_LABELS;
        ALL_GENERATORS = FALLBACK_ALL_GENERATORS;
        SUPPORTED_TYPES = FALLBACK_SUPPORTED_TYPES;
        DICTIONARIES = DICTIONARIES.length ? DICTIONARIES : [{ value: 'mac_pool_4', label: 'mac_pool_4 (4)' }, { value: 'servers', label: 'servers (3)' }, { value: 'regions', label: 'regions (4)' }];
        REFERENCE_READY = true;
        return false;
    }
}

let fieldsState = [];
let selectedFieldIndex = -1;
let fieldParams = {};

document.addEventListener('DOMContentLoaded', async () => {
    const fieldsBody = document.getElementById('fieldsBody');
    fieldsBody.innerHTML = '<tr><td colspan="6" class="preview-placeholder">Загрузка справочников...</td></tr>';

    const fromApi = await loadReferenceData();
    const statusEl = document.getElementById('apiStatus');
    if (statusEl) statusEl.textContent = fromApi
        ? 'Справочники загружены с API'
        : 'Справочники из резервных данных. Откройте страницу через http://127.0.0.1:5000/';

    fieldsState = [
        { name: 'id', type: 'UUID', mode: 'random', generatorKind: 'uuid4', params: {} },
        { name: 'value', type: 'Int32', mode: 'random', generatorKind: 'random_int', params: { min: 0, max: 100 } }
    ];
    renderFields();
    selectedFieldIndex = 0;
    updateFieldParamsPanel();

    document.getElementById('btnAddField').addEventListener('click', addField);
    document.getElementById('generatePreview').addEventListener('click', generatePreview);
    document.getElementById('btnSave').addEventListener('click', saveAsTemplate);
    document.getElementById('btnLoadTables').addEventListener('click', loadTables);
    document.getElementById('btnClearTable').addEventListener('click', clearTable);
    document.getElementById('btnDropTable').addEventListener('click', dropTable);
    document.getElementById('btnRefreshPreview').addEventListener('click', refreshPreview);

    document.querySelectorAll('input[name="volumeMode"]').forEach(r => {
        r.addEventListener('change', () => {
            const mode = r.value;
            document.getElementById('volumeRowsWrap').style.display = mode === 'rows' ? '' : 'none';
            document.getElementById('volumeDurationWrap').style.display = mode === 'duration' ? '' : 'none';
            if (mode === 'duration') updateDurationRowsHint();
        });
    });
    ['durationValue', 'durationUnit', 'ratePerSecond'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('input', updateDurationRowsHint);
        if (el) el.addEventListener('change', updateDurationRowsHint);
    });

    document.getElementById('tableSelect').addEventListener('change', () => {
        const sel = document.getElementById('tableSelect');
        const wrap = document.getElementById('targetTableWrap');
        const actions = document.getElementById('tableActions');
        wrap.style.display = sel.value === '__new__' ? '' : 'none';
        actions.style.display = sel.value !== '__new__' ? '' : 'none';
        if (sel.value !== '__new__') loadTablePreview(sel.value);
    });

    document.querySelectorAll('input[name="dbEngine"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const port = document.getElementById('dbPort');
            const user = document.getElementById('dbUser');
            const pass = document.getElementById('dbPassword');
            if (radio.value === 'postgres') {
                port.value = '5433';
                user.value = 'postgres';
                pass.value = '';
            } else {
                port.value = '18123';
                user.value = 'default';
                pass.value = 'ch_pass';
            }
            resetTableSelectForEngine();
        });
    });
});

function getDefaultParams(kind) {
    const params = GENERATOR_PARAMS[kind] || {};
    const out = {};
    Object.entries(params).forEach(([k, v]) => {
        if (v.default !== undefined) out[k] = v.default;
    });
    return out;
}

function getGeneratorsForTypeMode(type, mode) {
    const list = DATA_TYPE_GENERATORS[type]?.[mode] || [];
    return (ALL_GENERATORS.length ? ALL_GENERATORS : FALLBACK_ALL_GENERATORS).filter(g => list.includes(g.kind));
}

function renderFields() {
    const tbody = document.getElementById('fieldsBody');
    tbody.innerHTML = '';
    const types = SUPPORTED_TYPES.length ? SUPPORTED_TYPES : FALLBACK_SUPPORTED_TYPES;
    const modes = [
        { id: 'random', label: 'Случайное' },
        { id: 'sequential', label: 'Последовательное' },
        { id: 'fixed', label: 'Фиксированный список' }
    ];
    fieldsState.forEach((f, idx) => {
        const tr = document.createElement('tr');
        tr.dataset.index = idx;
        tr.className = 'field-row';
        const nameValid = f.name && f.name.trim() && f.name.trim().toLowerCase() !== 'field';
        if (!nameValid) tr.classList.add('field-row-invalid');
        const fType = f.type || 'String';
        const fMode = f.mode || 'random';
        const availGens = getGeneratorsForTypeMode(fType, fMode);
        tr.innerHTML = `
            <td class="col-drag">⋮⋮</td>
            <td>
                <input type="text" class="field-input-name" placeholder="field_name" value="${escapeHtml(f.name)}" data-index="${idx}">
            </td>
            <td>
                <select class="field-select-type" data-index="${idx}">
                    ${types.map(t => `<option value="${t.id}" ${fType === t.id ? 'selected' : ''}>${escapeHtml(t.label)}</option>`).join('')}
                </select>
            </td>
            <td>
                <select class="field-select-mode" data-index="${idx}">
                    ${modes.map(m => `<option value="${m.id}" ${fMode === m.id ? 'selected' : ''}>${escapeHtml(m.label)}</option>`).join('')}
                </select>
            </td>
            <td>
                <select class="field-select-gen" data-index="${idx}">
                    <option value="">Выберите генератор</option>
                    ${availGens.map(g => `<option value="${g.kind}" ${f.generatorKind === g.kind ? 'selected' : ''}>${escapeHtml(g.label)}</option>`).join('')}
                </select>
            </td>
            <td class="col-remove">
                <button type="button" class="btn-remove-field" data-index="${idx}" title="Удалить">×</button>
            </td>
        `;
        tr.querySelector('.field-input-name').addEventListener('input', (e) => {
            fieldsState[idx].name = e.target.value;
            e.target.closest('tr').classList.toggle('field-row-invalid', !e.target.value.trim() || e.target.value.trim().toLowerCase() === 'field');
        });
        tr.querySelector('.field-input-name').addEventListener('focus', () => { selectedFieldIndex = idx; updateFieldParamsPanel(); });
        tr.querySelector('.field-select-type').addEventListener('focus', () => { selectedFieldIndex = idx; updateFieldParamsPanel(); });
        tr.querySelector('.field-select-mode').addEventListener('focus', () => { selectedFieldIndex = idx; updateFieldParamsPanel(); });
        tr.querySelector('.field-select-type').addEventListener('change', (e) => {
            fieldsState[idx].type = e.target.value;
            const newAvail = getGeneratorsForTypeMode(fieldsState[idx].type, fieldsState[idx].mode);
            if (!newAvail.some(g => g.kind === fieldsState[idx].generatorKind)) {
                fieldsState[idx].generatorKind = newAvail[0]?.kind || null;
                fieldsState[idx].params = fieldsState[idx].generatorKind ? getDefaultParams(fieldsState[idx].generatorKind) : {};
            }
            renderFields();
            updateFieldParamsPanel();
        });
        tr.querySelector('.field-select-mode').addEventListener('change', (e) => {
            fieldsState[idx].mode = e.target.value;
            const newAvail = getGeneratorsForTypeMode(fieldsState[idx].type, fieldsState[idx].mode);
            if (!newAvail.some(g => g.kind === fieldsState[idx].generatorKind)) {
                fieldsState[idx].generatorKind = newAvail[0]?.kind || null;
                fieldsState[idx].params = fieldsState[idx].generatorKind ? getDefaultParams(fieldsState[idx].generatorKind) : {};
            }
            renderFields();
            updateFieldParamsPanel();
        });
        tr.querySelector('.field-select-gen').addEventListener('change', (e) => {
            const kind = e.target.value;
            fieldsState[idx].generatorKind = kind || null;
            fieldsState[idx].params = kind ? getDefaultParams(kind) : {};
            selectedFieldIndex = idx;
            updateFieldParamsPanel();
        });
        tr.querySelector('.field-select-gen').addEventListener('focus', () => { selectedFieldIndex = idx; updateFieldParamsPanel(); });
        tr.querySelector('.btn-remove-field').addEventListener('click', () => removeField(idx));
        tbody.appendChild(tr);
    });
}

function addField() {
    fieldsState.push({ name: '', type: 'String', mode: 'random', generatorKind: null, params: {} });
    renderFields();
    selectedFieldIndex = fieldsState.length - 1;
    updateFieldParamsPanel();
}

function removeField(idx) {
    fieldsState.splice(idx, 1);
    if (selectedFieldIndex >= fieldsState.length) selectedFieldIndex = Math.max(0, fieldsState.length - 1);
    renderFields();
    updateFieldParamsPanel();
}

function updateFieldParamsPanel() {
    const panel = document.getElementById('fieldParamsPanel');
    const content = document.getElementById('fieldParamsContent');
    if (selectedFieldIndex < 0 || selectedFieldIndex >= fieldsState.length) {
        panel.style.display = 'none';
        return;
    }
    const f = fieldsState[selectedFieldIndex];
    if (!f.generatorKind) {
        panel.style.display = 'none';
        return;
    }
    panel.style.display = 'block';
    content.innerHTML = '';
    const params = GENERATOR_PARAMS[f.generatorKind] || {};
    Object.entries(params).forEach(([key, config]) => {
        const paramGroup = document.createElement('div');
        paramGroup.className = 'param-group';
        if (f.generatorKind === 'random_int' && key === 'precision') paramGroup.dataset.showWhen = 'use_float';
        if (f.generatorKind === 'regex' && key === 'pattern') paramGroup.dataset.showWhen = 'preset_empty';
        if (f.generatorKind === 'enum_choice' && key === 'weights') paramGroup.dataset.showWhen = 'enum_mode_random';
        if (f.generatorKind === 'enum_choice' && key === 'dictionary') paramGroup.dataset.showWhen = 'enum_source_dict';
        if (f.generatorKind === 'enum_choice' && key === 'values') paramGroup.dataset.showWhen = 'enum_source_inline';
        const label = document.createElement('label');
        label.textContent = config.label || key;
        let input;
        if (config.type === 'textarea') {
            input = document.createElement('textarea');
            input.placeholder = config.placeholder || '';
            const val = f.params[key];
            input.value = Array.isArray(val) ? val.join('\n') : (val ?? config.default ?? '');
        } else if (config.type === 'select' && config.options) {
            input = document.createElement('select');
            const labels = config.optionLabels || {};
            config.options.forEach(opt => {
                const o = document.createElement('option');
                o.value = opt;
                o.textContent = (labels[opt] != null ? labels[opt] : opt) || '(пусто)';
                if (String(opt) === String(f.params[key] ?? config.default ?? '')) o.selected = true;
                input.appendChild(o);
            });
        } else if (config.type === 'dictionary_select') {
            input = document.createElement('select');
            const opt0 = document.createElement('option');
            opt0.value = '';
            opt0.textContent = '— Выберите —';
            input.appendChild(opt0);
            (DICTIONARIES || []).forEach(d => {
                const o = document.createElement('option');
                o.value = d.value || d.name;
                o.textContent = d.label || d.name;
                if (String(o.value) === String(f.params[key] ?? config.default ?? '')) o.selected = true;
                input.appendChild(o);
            });
        } else if (config.type === 'checkbox') {
            input = document.createElement('input');
            input.type = 'checkbox';
            input.checked = f.params[key] ?? config.default ?? false;
        } else {
            input = document.createElement('input');
            input.type = config.type === 'text' ? 'text' : config.type;
            input.placeholder = config.placeholder || '';
            const val = f.params[key] ?? config.default;
            input.value = config.type === 'number' ? (val ?? 0) : (val ?? '');
            if (config.type === 'number') {
                if (config.min !== undefined) input.min = config.min;
                if (config.max !== undefined) input.max = config.max;
                if (config.step !== undefined) input.step = config.step;
            }
        }
        input.dataset.paramKey = key;
        const eventType = (config.type === 'select' || config.type === 'checkbox' || config.type === 'dictionary_select') ? 'change' : 'input';
        input.addEventListener(eventType, () => {
            fieldsState[selectedFieldIndex].params[key] = getParamValue(input, config.type);
            if (f.generatorKind === 'random_int' && key === 'use_float') {
                const prec = content.querySelector('[data-show-when="use_float"]');
                if (prec) prec.style.display = input.checked ? '' : 'none';
            }
            if (f.generatorKind === 'regex' && key === 'preset') {
                const pat = content.querySelector('[data-show-when="preset_empty"]');
                if (pat) pat.style.display = (input.value === '' || input.value === undefined) ? '' : 'none';
            }
            if (f.generatorKind === 'enum_choice' && key === 'mode') {
                const w = content.querySelector('[data-show-when="enum_mode_random"]');
                if (w) w.style.display = (input.value === 'random') ? '' : 'none';
            }
            if (f.generatorKind === 'enum_choice' && key === 'source') {
                const dictDiv = content.querySelector('[data-show-when="enum_source_dict"]');
                const valsDiv = content.querySelector('[data-show-when="enum_source_inline"]');
                if (dictDiv) dictDiv.style.display = (input.value === 'dictionary') ? '' : 'none';
                if (valsDiv) valsDiv.style.display = (input.value === 'inline') ? '' : 'none';
            }
        });
        paramGroup.appendChild(label);
        paramGroup.appendChild(input);
        content.appendChild(paramGroup);
        if (paramGroup.dataset.showWhen === 'use_float') {
            const useFloat = content.querySelector(`[data-param-key="use_float"]`);
            paramGroup.style.display = (useFloat && useFloat.checked) ? '' : 'none';
        }
        if (paramGroup.dataset.showWhen === 'preset_empty') {
            const presetSel = content.querySelector(`[data-param-key="preset"]`);
            const presetVal = presetSel ? presetSel.value : (f.params.preset ?? '');
            paramGroup.style.display = (!presetVal || presetVal === '') ? '' : 'none';
        }
        if (paramGroup.dataset.showWhen === 'enum_mode_random') {
            const modeSel = content.querySelector(`[data-param-key="mode"]`);
            const modeVal = modeSel ? modeSel.value : (f.params.mode ?? 'random');
            paramGroup.style.display = (modeVal === 'random') ? '' : 'none';
        }
        if (paramGroup.dataset.showWhen === 'enum_source_dict') {
            const srcSel = content.querySelector(`[data-param-key="source"]`);
            const srcVal = srcSel ? srcSel.value : (f.params.source ?? 'inline');
            paramGroup.style.display = (srcVal === 'dictionary') ? '' : 'none';
        }
        if (paramGroup.dataset.showWhen === 'enum_source_inline') {
            const srcSel = content.querySelector(`[data-param-key="source"]`);
            const srcVal = srcSel ? srcSel.value : (f.params.source ?? 'inline');
            paramGroup.style.display = (srcVal === 'inline') ? '' : 'none';
        }
    });
}

function collectFieldsForRequest() {
    const gens = ALL_GENERATORS.length ? ALL_GENERATORS : FALLBACK_ALL_GENERATORS;
    return fieldsState
        .filter(f => f.name && f.name.trim() && f.generatorKind)
        .map(f => {
            const g = gens.find(x => x.kind === f.generatorKind);
            let fType = f.type || (g?.defaultType) || 'String';
            const bp = convertParamsForBackend(f.generatorKind, f.params);
            if (f.generatorKind === 'random_int' && bp.use_float && fType === 'Int32') fType = 'Float32';
            return {
                name: f.name.trim(),
                type: fType,
                generator_kind: f.generatorKind,
                generator_params: bp
            };
        });
}

function getParamValue(input, type) {
    if (type === 'number') {
        return parseFloat(input.value) || 0;
    } else if (type === 'textarea') {
        return input.value.split('\n').filter(v => v.trim()).map(v => v.trim());
    } else if (type === 'select' || type === 'dictionary_select') {
        return input.value;
    } else if (type === 'checkbox') {
        return input.checked;
    }
    return input.value;
}

function getConnection() {
    const engine = document.querySelector('input[name="dbEngine"]:checked')?.value || 'clickhouse';
    return {
        engine,
        host: document.getElementById('dbHost').value || 'localhost',
        port: parseInt(document.getElementById('dbPort').value) || (engine === 'postgres' ? 5433 : 18123),
        username: document.getElementById('dbUser').value || (engine === 'postgres' ? 'postgres' : 'default'),
        password: document.getElementById('dbPassword').value || (engine === 'postgres' ? '' : 'ch_pass'),
        database: document.getElementById('dbDatabase').value || 'default',
        secure: false
    };
}

function getTargetTable() {
    const sel = document.getElementById('tableSelect');
    return sel.value === '__new__' ? (document.getElementById('targetTable').value?.trim() || 'preview_table') : sel.value;
}

function resetTableSelectForEngine() {
    const sel = document.getElementById('tableSelect');
    const wrap = document.getElementById('targetTableWrap');
    const infoEl = document.getElementById('tableListInfo');
    const actions = document.getElementById('tableActions');
    sel.innerHTML = '<option value="__new__">➕ Создать новую</option>';
    sel.value = '__new__';
    wrap.style.display = '';
    infoEl.style.display = 'none';
    actions.style.display = 'none';
}

async function clearTable() {
    const table = getTargetTable();
    if (table && document.getElementById('tableSelect').value === '__new__') return;
    if (!table) {
        alert('Выберите таблицу из списка');
        return;
    }
    if (!confirm(`Очистить таблицу «${table}»? (TRUNCATE — удалятся все строки, схема сохранится)`)) return;
    try {
        const res = await fetch('/api/clear-table', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ connection: getConnection(), table })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail?.error || data.detail || 'Ошибка');
        if (!data.success) throw new Error(data.message || 'Ошибка');
        alert(data.message || 'Таблица очищена');
    } catch (e) {
        alert('Ошибка: ' + (e.message || e));
    }
}

async function dropTable() {
    const table = getTargetTable();
    if (table && document.getElementById('tableSelect').value === '__new__') return;
    if (!table) {
        alert('Выберите таблицу из списка');
        return;
    }
    if (!confirm(`Удалить таблицу «${table}»? (DROP TABLE — таблица будет удалена полностью. Это действие необратимо.)`)) return;
    try {
        const res = await fetch('/api/drop-table', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ connection: getConnection(), table })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail?.error || data.detail || 'Ошибка');
        if (!data.success) throw new Error(data.message || 'Ошибка');
        alert(data.message || 'Таблица удалена');
        resetTableSelectForEngine();
        loadTables();
    } catch (e) {
        alert('Ошибка: ' + (e.message || e));
    }
}

async function loadTables() {
    const btn = document.getElementById('btnLoadTables');
    const sel = document.getElementById('tableSelect');
    const connection = getConnection();
    btn.disabled = true;
    btn.textContent = 'Загрузка...';
    try {
        const res = await fetch('/api/list-tables', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ connection })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail?.error || (typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail)) || 'Ошибка загрузки');
        if (!data.success) throw new Error(data.message || 'Ошибка загрузки');

        const tables = data.tables || [];
        const engine = data.engine || connection.engine || 'clickhouse';
        const database = data.database || connection.database || 'default';
        sel.innerHTML = '<option value="__new__">➕ Создать новую</option>';
        tables.forEach(t => {
            const opt = document.createElement('option');
            opt.value = t;
            opt.textContent = t;
            sel.appendChild(opt);
        });
        const infoEl = document.getElementById('tableListInfo');
        const engineLabel = engine === 'postgres' ? 'PostgreSQL' : 'ClickHouse';
        const count = tables.length;
        const tablesWord = count === 1 ? 'таблица' : count >= 2 && count <= 4 ? 'таблицы' : 'таблиц';
        infoEl.textContent = `${engineLabel}: ${database} (${count} ${tablesWord})`;
        infoEl.style.display = '';
        document.getElementById('tableActions').style.display = sel.value !== '__new__' ? '' : 'none';
    } catch (e) {
        alert('Ошибка загрузки таблиц: ' + (e.message || e));
        document.getElementById('tableListInfo').style.display = 'none';
    } finally {
        btn.disabled = false;
        btn.textContent = 'Загрузить';
    }
}

function getPreviewLimit() {
    return parseInt(document.getElementById('previewLimit').value) || 100;
}

function getVolumeParams() {
    const mode = document.querySelector('input[name="volumeMode"]:checked')?.value || 'rows';
    if (mode === 'duration') {
        const val = parseFloat(document.getElementById('durationValue').value) || 60;
        const unit = document.getElementById('durationUnit').value;
        const rate = parseFloat(document.getElementById('ratePerSecond').value) || 10;
        const duration = `${val}${unit}`;
        return { duration, rate_per_second: rate, rows: null };
    }
    const rows = parseInt(document.getElementById('rowsToGenerate').value) || 10;
    return { rows, duration: null, rate_per_second: null };
}

function updateDurationRowsHint() {
    const hint = document.getElementById('durationRowsHint');
    if (!hint) return;
    const val = parseFloat(document.getElementById('durationValue').value) || 60;
    const unit = document.getElementById('durationUnit').value;
    const rate = parseFloat(document.getElementById('ratePerSecond').value) || 10;
    const mult = unit === 's' ? 1 : unit === 'm' ? 60 : 3600;
    const rows = Math.round(val * mult * rate);
    const etaSec = rate > 0 ? Math.ceil(rows / rate) : 0;
    let txt = `≈ ${rows.toLocaleString()} строк`;
    if (rate > 0 && rate < 10 && rows > 10) {
        txt += ` (~${etaSec} сек)`;
    }
    hint.textContent = txt;
}

function renderPreviewTable(fetchResult) {
    let html = '<table><thead><tr>';
    fetchResult.columns.forEach(col => {
        html += `<th>${escapeHtml(col)}</th>`;
    });
    html += '</tr></thead><tbody>';
    fetchResult.data.forEach(row => {
        html += '<tr>';
        row.forEach(cell => {
            html += `<td>${escapeHtml(String(cell))}</td>`;
        });
        html += '</tr>';
    });
    html += '</tbody></table>';
    if (fetchResult.total_rows !== null) {
        html += `<div class="preview-total">Всего строк в таблице: ${fetchResult.total_rows}</div>`;
    }
    return html;
}

async function loadTablePreview(tableName, shuffle = false) {
    const previewTable = document.getElementById('previewTable');
    const connection = getConnection();
    const limit = getPreviewLimit();
    previewTable.innerHTML = '<div class="preview-placeholder">Загрузка данных...</div>';
    try {
        const res = await fetch('/api/fetch-data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                connection,
                table: tableName,
                limit,
                shuffle,
                float_precision: 2
            })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail?.error || data.detail || 'Ошибка получения данных');
        if (!data.success) throw new Error(data.message || 'Ошибка');
        previewTable.innerHTML = renderPreviewTable(data);
    } catch (e) {
        previewTable.innerHTML = `<div class="preview-placeholder" style="color: #e74c3c;">Ошибка: ${escapeHtml(e.message || e)}</div>`;
    }
}

async function refreshPreview() {
    const sel = document.getElementById('tableSelect');
    const table = sel.value === '__new__' ? null : sel.value;
    if (!table) {
        const manualTable = document.getElementById('targetTable').value?.trim();
        if (manualTable) {
            await loadTablePreview(manualTable, false);
        } else {
            alert('Выберите таблицу из списка или укажите имя для «Создать новую»');
        }
        return;
    }
    await loadTablePreview(table, false);
}

async function generatePreview() {
    const fieldsData = collectFieldsForRequest();
    if (!fieldsData.length) {
        alert('Добавьте хотя бы одно поле с названием и выбранным генератором');
        return;
    }

    const previewTable = document.getElementById('previewTable');
    const connection = getConnection();
    const targetTable = getTargetTable();
    const createTable = document.getElementById('createTable').checked;
    const vol = getVolumeParams();

    if (!targetTable) {
        alert('Укажите название таблицы');
        return;
    }
    const rowsToGenerate = vol.rows;
    if (rowsToGenerate !== null && (rowsToGenerate < 1 || rowsToGenerate > 10000000)) {
        alert('Количество строк должно быть от 1 до 10,000,000');
        return;
    }
    if (vol.duration && (vol.rate_per_second < 0.01 || vol.rate_per_second > 10000)) {
        alert('Скорость должна быть от 0.01 до 10,000 сообщ/сек');
        return;
    }

    previewTable.innerHTML = '<div class="preview-placeholder">Генерация данных в БД...</div>';

    const body = {
        generator_kind: null,
        generator_params: null,
        fields: fieldsData,
        connection,
        target_table: targetTable,
        create_table: createTable,
        preview_only: false
    };
    if (vol.rows !== null) {
        body.rows = vol.rows;
        body.batch_size = Math.min(1000, vol.rows);
    } else {
        body.duration = vol.duration;
        body.rate_per_second = vol.rate_per_second;
        body.rows = 1;
        body.batch_size = 1000;
    }

    try {
        const generateResponse = await fetch('/api/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });

        const generateResult = await generateResponse.json();
        if (!generateResponse.ok) {
            throw new Error(generateResult.detail?.error || generateResult.detail || 'Ошибка генерации');
        }
        if (!generateResult.success) {
            throw new Error(generateResult.message || 'Ошибка генерации');
        }

        previewTable.innerHTML = '<div class="preview-placeholder">Загрузка данных из БД...</div>';
        const hasFloat = fieldsData.some(f => f.generator_kind === 'random_int' && f.generator_params?.use_float);
        const precision = hasFloat ? 2 : 2;
        const previewLimit = getPreviewLimit();
        const fetchResponse = await fetch('/api/fetch-data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                connection,
                table: targetTable,
                limit: previewLimit,
                shuffle: true,
                float_precision: precision
            })
        });

        const fetchResult = await fetchResponse.json();
        if (!fetchResponse.ok) {
            throw new Error(fetchResult.detail?.error || fetchResult.detail || 'Ошибка получения данных');
        }
        if (!fetchResult.success) {
            throw new Error('Ошибка получения данных из БД');
        }

        previewTable.innerHTML = renderPreviewTable(fetchResult);
    } catch (error) {
        previewTable.innerHTML = `<div class="preview-placeholder" style="color: #e74c3c;">Ошибка: ${escapeHtml(error.message)}</div>`;
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatCellForDisplay(cell, precision) {
    if (precision == null) return cell;
    const n = parseFloat(cell);
    if (!isNaN(n) && cell.includes('.')) {
        return n.toFixed(precision);
    }
    return cell;
}

function saveAsTemplate() {
    const fieldsData = collectFieldsForRequest();
    if (!fieldsData.length) {
        alert('Добавьте хотя бы одно поле с названием и выбранным генератором');
        return;
    }
    const orderCols = fieldsData.map(f => f.name).join(', ');
    const profile = {
        connection: { host: 'localhost', port: 18123, username: 'default', password: '', database: 'default', secure: false },
        target: { database: 'default', table: 'table_name', order_by: `(${orderCols})`, partition_by: null },
        fields: fieldsData.map(f => ({
            name: f.name,
            type: f.type,
            generator: { kind: f.generator_kind, params: f.generator_params }
        }))
    };
    const blob = new Blob([JSON.stringify(profile, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `profile_${Date.now()}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    alert('Профиль сохранен как шаблон!');
}

function convertParamsForBackend(generatorKind, params) {
    const backendParams = {};
    switch (generatorKind) {
        case 'random_int':
            return params;
        case 'sequence_int': {
            const result = { ...params };
            if (params.probability !== undefined) {
                result.probability = Math.round(params.probability * 100) / 10000;
            }
            return result;
        }
        case 'timestamp_asc':
        case 'timestamp_desc':
            return {
                start: params.start || 'now',
                step: params.step || '1s'
            };
        case 'random_digits':
        case 'uuid4':
        case 'url_template':
            return params;
        case 'regex': {
            const result = {};
            if (params.preset && params.preset.trim()) {
                result.preset = params.preset.trim();
            } else if (params.pattern && params.pattern.trim()) {
                result.pattern = params.pattern.trim();
            } else {
                result.pattern = params.pattern || '[a-z0-9]{8}';
            }
            return result;
        }
        case 'enum_choice': {
            const source = params.source || 'inline';
            const result = { mode: params.mode || 'random' };
            if (source === 'dictionary' && params.dictionary) {
                result.source = 'dictionary';
                result.dictionary = params.dictionary.trim();
            } else {
                result.source = 'inline';
                result.values = params.values || [];
                if (result.mode === 'random' && params.weights && params.weights.length > 0) {
                    result.weights = params.weights.map(w => Math.round((parseFloat(w) || 0) * 100) / 100);
                }
            }
            return result;
        }
        default:
            return params;
    }
}
