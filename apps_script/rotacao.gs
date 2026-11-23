/**
 * Rotação automática das planilhas históricas dos pipelines cda_*.
 * Roda na SUA conta do Google (usa o seu espaço no Drive, gratuito).
 *
 * Todo dia às ~22h (horário de Brasília), para cada planilha abaixo:
 *   se passou do limite de linhas ->
 *     1. copia o arquivo inteiro como "<nome> rev000" (rev001, rev002...) na mesma pasta;
 *     2. confere se a cópia tem o mesmo número de linhas;
 *     3. só então apaga as linhas de dados da planilha ativa (cabeçalho, ID e link continuam).
 *   Se algo falhar, nada é apagado.
 *
 * INSTALAÇÃO: cole este arquivo, selecione a função "instalar" e clique em Executar.
 * Ver o que aconteceu: menu "Execuções" (ícone de lista, à esquerda).
 */
var PLANILHAS = [
  { nome: 'trends (online_skf)',        id: '1uekxFKio9llwhP9CljTappvO3XaXrrV7g_WT2V3buIo', limite: 1000000 },
  { nome: 'measurements (offline_skf)', id: '1UK6AatDxCdqg8NZxgL8ZThyCSNXOh03_AY4jtGaumXc', limite: 600000 },
  { nome: 'conditions (offline_skf)',   id: '1-Hkx_2B5HauY71j0RDYXKS_09Ax34J0Dp6wUGRG32q4', limite: 300000 },
  { nome: 'telemetria (dynamox)',       id: '11rzDwr5U6HAfIUXHLNOCcoFkmPEcOUNDpTnZ4Nmg2uA', limite: 1000000 },
  { nome: 'results (lub_ext_als)',      id: '1aSpmZ10Z_xECpfFM0oceQcJ47MLrhz4RKoizOt7QwDk', limite: 15000 }
];
var ABA = 'Sheet1';

/** Rode UMA vez: autoriza, agenda a execução diária e já roda a primeira rotação. */
function instalar() {
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === 'rotacionarTodas') ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger('rotacionarTodas').timeBased().everyDays(1).atHour(22)
    .inTimezone('America/Sao_Paulo').create();
  Logger.log('Agendado: todo dia às 22h (Brasília).');
  rotacionarTodas();
}

function rotacionarTodas() {
  PLANILHAS.forEach(function (cfg) {
    try {
      rotacionar_(cfg);
    } catch (err) {
      Logger.log('[' + cfg.nome + '] ERRO (nada foi apagado): ' + err);
    }
  });
}

function rotacionar_(cfg) {
  var planilha = SpreadsheetApp.openById(cfg.id);
  var aba = planilha.getSheetByName(ABA);
  var ultima = aba.getLastRow();          // inclui o cabeçalho
  var linhas = Math.max(ultima - 1, 0);
  if (linhas <= cfg.limite) {
    Logger.log('[' + cfg.nome + '] ' + linhas + ' linhas — abaixo do limite de ' + cfg.limite);
    return;
  }

  // 1. cópia do arquivo inteiro
  var arquivo = DriveApp.getFileById(cfg.id);
  var pais = arquivo.getParents();
  var pasta = pais.hasNext() ? pais.next() : DriveApp.getRootFolder();
  var nomeRev = proximoNomeRev_(arquivo.getName(), pasta);
  var copia = arquivo.makeCopy(nomeRev, pasta);
  SpreadsheetApp.flush();

  // 2. conferência
  var naCopia = SpreadsheetApp.openById(copia.getId()).getSheetByName(ABA).getLastRow();
  if (naCopia !== ultima) {
    throw new Error('cópia "' + nomeRev + '" tem ' + naCopia + ' linhas, original ' + ultima);
  }

  // 3. esvazia a ativa (mantém cabeçalho)
  var max = aba.getMaxRows();
  if (max > 2) aba.deleteRows(3, max - 2);
  aba.getRange(2, 1, 1, aba.getMaxColumns()).clearContent();
  Logger.log('[' + cfg.nome + '] arquivada como "' + nomeRev + '" (' + linhas + ' linhas). Ativa esvaziada.');
}

function proximoNomeRev_(base, pasta) {
  var maior = -1;
  var arquivos = pasta.searchFiles('title contains "' + base.replace(/"/g, '\\"') + ' rev" and trashed = false');
  var re = new RegExp('^' + base.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ' rev(\\d{3,})$');
  while (arquivos.hasNext()) {
    var m = re.exec(arquivos.next().getName());
    if (m) maior = Math.max(maior, parseInt(m[1], 10));
  }
  return base + ' rev' + ('00' + (maior + 1)).slice(-3);
}
