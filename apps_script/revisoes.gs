/**
 * Apps Script de apoio aos pipelines cda_* — roda na SUA conta do Google.
 *
 * Por quê: a conta de serviço usada pelo GitHub Actions não tem espaço no
 * Drive e não consegue criar cópias. Este script cria as cópias (rev000,
 * rev001, backups) com a sua conta quando o pipeline pede.
 *
 * Segurança (não precisa configurar nada): cada pedido traz um token
 * assinado pelo Google. O script só atende se o token for de uma conta de
 * serviço que JÁ É EDITORA da planilha pedida. Ele só copia arquivos (nunca
 * altera o original) e só apaga cópias de teste.
 *
 * Instalação: cole este arquivo em script.google.com e publique como
 * App da Web (Executar como: Eu | Quem pode acessar: Qualquer pessoa).
 */
function doGet(e) {
  var p = (e && e.parameter) || {};
  try {
    var email = emailDoToken_(p.id_token);
    if (!email) return resposta_({ ok: false, erro: 'nao autorizado' });

    if (p.acao === 'ping') return resposta_({ ok: true, conta: email });

    var arquivo = DriveApp.getFileById(p.id);
    if (!ehEditor_(arquivo, email)) {
      return resposta_({ ok: false, erro: 'conta sem acesso de edicao a este arquivo' });
    }

    if (p.acao === 'copiar') {
      var pais = arquivo.getParents();
      var pasta = pais.hasNext() ? pais.next() : DriveApp.getRootFolder();
      var copia = arquivo.makeCopy(p.nome, pasta);
      copia.addEditor(email);   // a conta de serviço confere a cópia
      return resposta_({ ok: true, id: copia.getId(), nome: copia.getName() });
    }

    if (p.acao === 'apagar_teste') {
      if (arquivo.getName().indexOf('TESTE-COPIA') === -1) {
        return resposta_({ ok: false, erro: 'so apaga arquivos de teste' });
      }
      arquivo.setTrashed(true);
      return resposta_({ ok: true });
    }

    return resposta_({ ok: false, erro: 'acao invalida' });
  } catch (err) {
    return resposta_({ ok: false, erro: String(err) });
  }
}

/** Valida o token no Google e devolve o e-mail da conta de serviço (ou null). */
function emailDoToken_(idToken) {
  if (!idToken) return null;
  var r = UrlFetchApp.fetch(
    'https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(idToken),
    { muteHttpExceptions: true }
  );
  if (r.getResponseCode() !== 200) return null;
  var info = JSON.parse(r.getContentText());
  var email = String(info.email || '').toLowerCase();
  if (String(info.email_verified) !== 'true') return null;
  if (String(info.aud || '').indexOf('https://script.google.com/') !== 0) return null;
  if (!/\.gserviceaccount\.com$/.test(email)) return null;
  return email;
}

function ehEditor_(arquivo, email) {
  var editores = arquivo.getEditors().map(function (u) { return u.getEmail().toLowerCase(); });
  return editores.indexOf(email) !== -1 || arquivo.getOwner().getEmail().toLowerCase() === email;
}

function resposta_(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
