// OLD версия — одно поле, generator_kind + generator_params
let DATA_TYPE_GENERATORS = {};
let GENERATOR_PARAMS = {};
let GENERATOR_LABELS = {};
let ALL_GENERATORS = [];
let SUPPORTED_TYPES = [];
let fieldParamsState = {};

const FALLBACK_DATA_TYPE_GENERATORS = {
    'String': { random: ['random_digits', 'uuid4', 'url_template', 'enum_choice'], sequential: [], fixed: ['enum_choice'] },
    'Int32': { random: ['random_int'], sequential: ['sequence_int'], fixed: ['enum_choice'] },
    'DateTime': { random: ['timestamp_asc', 'timestamp_desc'], sequential: ['timestamp_asc', 'timestamp_desc'], fixed: ['enum_choice'] },
    'UUID': { random: ['uuid4'], sequential: [], fixed: [] }
};

const FALLBACK_GENERATOR_PARAMS = {
    'random_int': { min: { type: 'number', label: 'Минимум', default: 0 }, max: { type: 'number', label: 'Максимум', default: 100 }, use_float: { type: 'checkbox', label: 'Генерировать вещественные числа', default: false }, precision: { type: 'number', label: 'Знаков после запятой', default: 2, min: 0, max: 10 } },
    'sequence_int': { start: { type: 'number', label: 'Начальное значение', default: 0 }, step: { type: 'number', label: 'Шаг', default: 1 }, probability: { type: 'number', label: 'Вероятность (%)', default: 100, min: 0, max: 100, step: 0.01 } },
    'timestamp_asc': { start: { type: 'text', label: 'Начало', default: 'now' }, step: { type: 'text', label: 'Шаг', default: '1s' } },
    'timestamp_desc': { start: { type: 'text', label: 'Начало', default: 'now' }, step: { type: 'text', label: 'Шаг', default: '1s' } },
    'random_digits': { length: { type: 'number', label: 'Длина', default: 8, min: 1, max: 100 } },
    'uuid4': {},
    'url_template': { pattern: { type: 'text', label: 'Шаблон', default: 'https://example.com/item/{row}?uuid={uuid}' } },
    'enum_choice': { values: { type: 'textarea', label: 'Значения (по строке)', placeholder: 'value1\nvalue2' }, weights: { type: 'textarea', label: 'Вероятности % (опционально)' } }
};

const FALLBACK_GENERATOR_LABELS = {
    'random_int': 'Случайные числа', 'sequence_int': 'Последовательность', 'timestamp_asc': 'Даты по возрастанию',
    'timestamp_desc': 'Даты по убыванию', 'random_digits': 'Случайные цифры', 'uuid4': 'UUID',
    'url_template': 'URL шаблон', 'enum_choice': 'Список значений'
};

const SEQUENTIAL_KINDS = ['sequence_int', 'timestamp_asc', 'timestamp_desc'];
const FIXED_KINDS = ['enum_choice'];

function buildFromApi(generators, types) {
    const dtypeGen = {};
    const genParams = {};
    const genLabels = {};
    const allGens = [];
    for (const t of types) {
        dtypeGen[t.id] = { random: [], sequential: [], fixed: [] };
        for (const g of generators) {
            if (!g.compatible_types || !g.compatible_types.includes(t.id)) continue;
            if (FIXED_KINDS.includes(g.kind)) dtypeGen[t.id].fixed.push(g.kind);
            if (SEQUENTIAL_KINDS.includes(g.kind)) dtypeGen[t.id].sequential.push(g.kind);
            if (FIXED_KINDS.includes(g.kind) || !SEQUENTIAL_KINDS.includes(g.kind)) dtypeGen[t.id].random.push(g.kind);
        }
        dtypeGen[t.id].random = [...new Set(dtypeGen[t.id].random)];
    }
    for (const g of generators) {
        const label = g.description ? g.description.split('.')[0].trim() : g.kind;
        genLabels[g.kind] = label;
        allGens.push({ kind: g.kind, label, defaultType: (g.compatible_types && g.compatible_types[0]) || 'String' });
        genParams[g.kind] = {};
        for (const p of g.params || []) {
            let fType = p.type === 'string' ? 'text' : p.type === 'boolean' ? 'checkbox' : p.type === 'array' ? 'textarea' : p.type;
            genParams[g.kind][p.name] = { type: fType, label: p.description || p.name, default: p.default };
            if (p.placeholder) genParams[g.kind][p.name].placeholder = p.placeholder;
            if (p.min != null) genParams[g.kind][p.name].min = p.min;
            if (p.max != null) genParams[g.kind][p.name].max = p.max;
            if (p.step != null) genParams[g.kind][p.name].step = p.step;
        }
    }
    return { dtypeGen, genParams, genLabels, allGens };
}

async function loadReferenceData() {
    const ts = Date.now();
    try {
        const [genRes, typesRes] = await Promise.all([
            fetch(`/api/generators?_=${ts}`, { cache: 'no-store' }),
            fetch(`/api/supported-types?_=${ts}`, { cache: 'no-store' })
        ]);
        if (!genRes.ok || !typesRes.ok) throw new Error('API error');
        const genJson = await genRes.json();
        const typesJson = await typesRes.json();
        const generators = genJson.generators || [];
        const types = typesJson.types || [];
        if (!generators.length || !types.length) throw new Error('Empty');
        const { dtypeGen, genParams, genLabels, allGens } = buildFromApi(generators, types);
        DATA_TYPE_GENERATORS = dtypeGen;
        GENERATOR_PARAMS = genParams;
        GENERATOR_LABELS = genLabels;
        ALL_GENERATORS = allGens;
        SUPPORTED_TYPES = types;
        return true;
    } catch (e) {
        DATA_TYPE_GENERATORS = FALLBACK_DATA_TYPE_GENERATORS;
        GENERATOR_PARAMS = FALLBACK_GENERATOR_PARAMS;
        GENERATOR_LABELS = FALLBACK_GENERATOR_LABELS;
        ALL_GENERATORS = [
            { kind: 'uuid4', label: 'UUID', defaultType: 'UUID' },
            { kind: 'random_int', label: 'Случайные числа', defaultType: 'Int32' },
            { kind: 'sequence_int', label: 'Последовательность', defaultType: 'Int32' },
            { kind: 'random_digits', label: 'Случайные цифры', defaultType: 'String' },
            { kind: 'url_template', label: 'URL шаблон', defaultType: 'String' },
            { kind: 'timestamp_asc', label: 'Даты по возрастанию', defaultType: 'DateTime' },
            { kind: 'timestamp_desc', label: 'Даты по убыванию', defaultType: 'DateTime' },
            { kind: 'enum_choice', label: 'Список значений', defaultType: 'String' }
        ];
        SUPPORTED_TYPES = [{ id: 'String', label: 'Строка' }, { id: 'Int32', label: 'Число' }, { id: 'DateTime', label: 'Дата' }, { id: 'UUID', label: 'UUID' }];
        return false;
    }
}

function getDefaultParams(kind) {
    const params = GENERATOR_PARAMS[kind] || {};
    const out = {};
    Object.entries(params).forEach(([k, v]) => { if (v.default !== undefined) out[k] = v.default; });
    return out;
}

function getGeneratorsForTypeMode(type, mode) {
    const list = DATA_TYPE_GENERATORS[type]?.[mode] || [];
    return ALL_GENERATORS.filter(g => list.includes(g.kind));
}

function getFieldMode() {
    return document.querySelector('input[name="fieldMode"]:checked')?.value || 'random';
}

function renderGeneratorSelect() {
    const type = document.getElementById('fieldType').value;
    const mode = getFieldMode();
    const avail = getGeneratorsForTypeMode(type, mode);
    const sel = document.getElementById('fieldGenerator');
    const current = sel.value;
    sel.innerHTML = '<option value="">Выберите генератор</option>' + avail.map(g => 
        `<option value="${g.kind}" ${current === g.kind ? 'selected' : ''}>${escapeHtml(g.label)}</option>`
    ).join('');
    if (!avail.some(g => g.kind === current)) {
        const first = avail[0];
        if (first) {
            sel.value = first.kind;
            fieldParamsState[first.kind] = getDefaultParams(first.kind);
        }
    }
    updateFieldParamsPanel();
}

function updateFieldParamsPanel() {
    const kind = document.getElementById('fieldGenerator').value;
    const content = document.getElementById('fieldParamsContent');
    content.innerHTML = '';
    if (!kind) return;
    const params = GENERATOR_PARAMS[kind] || {};
    if (Object.keys(params).length === 0) return;
    if (!fieldParamsState[kind]) fieldParamsState[kind] = getDefaultParams(kind);
    const state = fieldParamsState[kind] || {};
    Object.entries(params).forEach(([key, config]) => {
        const div = document.createElement('div');
        div.className = 'param-group';
        if (kind === 'random_int' && key === 'precision') div.dataset.showWhen = 'use_float';
        const label = document.createElement('label');
        label.textContent = config.label || key;
        let input;
        if (config.type === 'textarea') {
            input = document.createElement('textarea');
            input.placeholder = config.placeholder || '';
            const val = state[key];
            input.value = Array.isArray(val) ? val.join('\n') : (val ?? config.default ?? '');
        } else if (config.type === 'checkbox') {
            input = document.createElement('input');
            input.type = 'checkbox';
            input.checked = state[key] ?? config.default ?? false;
        } else {
            input = document.createElement('input');
            input.type = config.type === 'text' ? 'text' : (config.type || 'text');
            input.placeholder = config.placeholder || '';
            const val = state[key] ?? config.default;
            input.value = config.type === 'number' ? (val ?? 0) : (val ?? '');
            if (config.type === 'number') {
                if (config.min !== undefined) input.min = config.min;
                if (config.max !== undefined) input.max = config.max;
                if (config.step !== undefined) input.step = config.step;
            }
        }
        input.dataset.paramKey = key;
        input.dataset.generatorKind = kind;
        const ev = (config.type === 'checkbox') ? 'change' : 'input';
        input.addEventListener(ev, () => {
            if (!fieldParamsState[kind]) fieldParamsState[kind] = {};
            fieldParamsState[kind][key] = getParamValue(input, config.type);
            if (kind === 'random_int' && key === 'use_float') {
                const precDiv = content.querySelector('[data-show-when="use_float"]');
                if (precDiv) precDiv.style.display = input.checked ? '' : 'none';
            }
        });
        div.appendChild(label);
        div.appendChild(input);
        content.appendChild(div);
        if (div.dataset.showWhen === 'use_float') {
            const useFloatInput = content.querySelector('[data-param-key="use_float"]');
            div.style.display = (useFloatInput && useFloatInput.checked) ? '' : 'none';
        }
    });
}

function getParamValue(input, type) {
    if (type === 'number') return parseFloat(input.value) || 0;
    if (type === 'textarea') return input.value.split('\n').filter(v => v.trim()).map(v => v.trim());
    if (type === 'checkbox') return input.checked;
    return input.value;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function convertParamsForBackend(kind, params) {
    const backendParams = { ...params };
    if (kind === 'sequence_int' && params.probability !== undefined) {
        backendParams.probability = Math.round(params.probability * 100) / 10000;
    }
    if (kind === 'enum_choice' && params.weights && params.weights.length > 0) {
        backendParams.weights = params.weights.map(w => Math.round((parseFloat(w) || 0) * 100) / 100);
    }
    return backendParams;
}

document.addEventListener('DOMContentLoaded', async () => {
    const fromApi = await loadReferenceData();
    const statusEl = document.getElementById('apiStatus');
    statusEl.textContent = fromApi ? 'Справочники загружены с API' : 'Справочники из резервных данных. OLD на порту 5051.';
    
    renderGeneratorSelect();

    document.getElementById('fieldType').addEventListener('change', () => {
        const mode = getFieldMode();
        const type = document.getElementById('fieldType').value;
        const avail = getGeneratorsForTypeMode(type, mode);
        const gen = avail[0];
        document.getElementById('fieldGenerator').value = gen?.kind || '';
        if (gen) fieldParamsState[gen.kind] = getDefaultParams(gen.kind);
        renderGeneratorSelect();
    });
    document.querySelectorAll('input[name="fieldMode"]').forEach(radio => {
        radio.addEventListener('change', () => {
            const type = document.getElementById('fieldType').value;
            const mode = getFieldMode();
            const avail = getGeneratorsForTypeMode(type, mode);
            const gen = avail[0];
            document.getElementById('fieldGenerator').value = gen?.kind || '';
            if (gen) fieldParamsState[gen.kind] = getDefaultParams(gen.kind);
            renderGeneratorSelect();
        });
    });
    document.getElementById('fieldGenerator').addEventListener('change', () => {
        const kind = document.getElementById('fieldGenerator').value;
        if (kind) fieldParamsState[kind] = getDefaultParams(kind);
        updateFieldParamsPanel();
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
        });
    });

    document.getElementById('generatePreview').addEventListener('click', generatePreview);
});

async function generatePreview() {
    const fieldName = document.getElementById('fieldName').value?.trim();
    const fieldType = document.getElementById('fieldType').value;
    const generatorKind = document.getElementById('fieldGenerator').value;
    if (!fieldName) {
        alert('Укажите название поля');
        return;
    }
    if (!generatorKind) {
        alert('Выберите генератор');
        return;
    }

    const previewTable = document.getElementById('previewTable');
    const engine = document.querySelector('input[name="dbEngine"]:checked')?.value || 'clickhouse';
    const connection = {
        engine,
        host: document.getElementById('dbHost').value || 'localhost',
        port: parseInt(document.getElementById('dbPort').value) || (engine === 'postgres' ? 5433 : 18123),
        username: document.getElementById('dbUser').value || (engine === 'postgres' ? 'postgres' : 'default'),
        password: document.getElementById('dbPassword').value || (engine === 'postgres' ? '' : 'ch_pass'),
        database: document.getElementById('dbDatabase').value || 'default',
        secure: false
    };
    const targetTable = document.getElementById('targetTable').value || 'preview_table';
    const createTable = document.getElementById('createTable').checked;
    const rowsToGenerate = parseInt(document.getElementById('rowsToGenerate').value) || 10;

    const params = fieldParamsState[generatorKind] || getDefaultParams(generatorKind);
    let effFieldType = fieldType;
    if (generatorKind === 'random_int' && params.use_float && fieldType === 'Int32') {
        effFieldType = 'Float32';
    }
    const generator_params = {
        field_name: fieldName,
        field_type: effFieldType,
        ...convertParamsForBackend(generatorKind, params)
    };

    previewTable.innerHTML = '<div class="preview-placeholder">Генерация данных в БД...</div>';

    try {
        const generateResponse = await fetch('/api/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                generator_kind: generatorKind,
                generator_params,
                connection,
                target_table: targetTable,
                rows: rowsToGenerate,
                batch_size: Math.min(1000, rowsToGenerate),
                create_table: createTable,
                preview_only: false
            })
        });

        const generateResult = await generateResponse.json();
        if (!generateResponse.ok) {
            throw new Error(generateResult.detail?.error || (typeof generateResult.detail === 'string' ? generateResult.detail : JSON.stringify(generateResult.detail)) || 'Ошибка генерации');
        }
        if (!generateResult.success) {
            throw new Error(generateResult.message || 'Ошибка генерации');
        }

        previewTable.innerHTML = '<div class="preview-placeholder">Загрузка данных из БД...</div>';
        const fetchResponse = await fetch('/api/fetch-data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                connection,
                table: targetTable,
                limit: 10,
                shuffle: true,
                float_precision: 2
            })
        });

        const fetchResult = await fetchResponse.json();
        if (!fetchResponse.ok) {
            throw new Error(fetchResult.detail?.error || 'Ошибка получения данных');
        }
        if (!fetchResult.success) {
            throw new Error('Ошибка получения данных из БД');
        }

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
            html += `<div style="margin-top: 12px; color: #666; font-size: 13px;">Всего строк в таблице: ${fetchResult.total_rows}</div>`;
        }
        previewTable.innerHTML = html;
    } catch (error) {
        previewTable.innerHTML = `<div class="preview-placeholder" style="color: #e74c3c;">Ошибка: ${escapeHtml(error.message)}</div>`;
    }
}
