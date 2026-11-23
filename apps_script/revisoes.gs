/**
 * Apps Script de apoio aos pipelines cda_* (roda na SUA conta do Google).
 *
 * Por quê: a conta de serviço usada pelo GitHub Actions não tem espaço no
 * Drive e não consegue criar cópias de arquivos. Este script faz a cópia
 * com a sua conta (seu espaço, gratuito) quando o pipeline pede.
 *
 * Instalação (uma vez):
 *  1. script.google.com > Novo projeto > cole este arquivo.
 *  2. Configurações do projeto (engrenagem) > Propriedades do script >
 *     adicionar propriedade TOKEN com o valor combinado (o mesmo do secret
 *     APPS_SCRIPT_TOKEN no GitHub).
 *  3. Implantar > Nova implantação > tipo "App da Web":
 *       Executar como: Eu   |   Quem pode acessar: Qualquer pessoa
 *     Autorize o acesso ao Drive quando pedir e copie a URL do app da Web.
 *  4. Coloque a URL no secret APPS_SCRIPT_URL dos repositórios.
 *
 * Segurança: sem o TOKEN correto nada é feito. O script só copia arquivos
 * (nunca altera o original) e só apaga cópias de teste.
 */
function doGet(e) {
  var p = (e && e.parameter) || {};
  var token = PropertiesService.getScriptProperties().getProperty('TOKEN');
  if (!token || p.token !== token) return resposta({ ok: false, erro: 'nao autorizado' });

  try {
    if (p.acao === 'ping') return resposta({ ok: true });

    if (p.acao === 'copiar') {
      var original = DriveApp.getFileById(p.id);
      var pais = original.getParents();
      var pasta = pais.hasNext() ? pais.next() : DriveApp.getRootFolder();
      var copia = original.makeCopy(p.nome, pasta);
      if (p.leitor) copia.addEditor(p.leitor);   // conta de serviço confere a cópia
      return resposta({ ok: true, id: copia.getId(), nome: copia.getName() });
    }

    if (p.acao === 'apagar_teste') {
      var f = DriveApp.getFileById(p.id);
      if (f.getName().indexOf('TESTE-COPIA') === -1) {
        return resposta({ ok: false, erro: 'so apaga arquivos de teste' });
      }
      f.setTrashed(true);
      return resposta({ ok: true });
    }

    return resposta({ ok: false, erro: 'acao invalida' });
  } catch (err) {
    return resposta({ ok: false, erro: String(err) });
  }
}

function resposta(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
