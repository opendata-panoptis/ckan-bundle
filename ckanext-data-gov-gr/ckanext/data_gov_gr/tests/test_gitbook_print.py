# -*- coding: utf-8 -*-
"""
Tests για τη σελίδα εκτύπωσης των οδηγών GitBook (λήψη PDF).

Όλες οι κλήσεις HTTP και η Redis είναι mocked· δεν χρειάζεται βάση.
"""
import re

import flask
import pytest
import requests
from werkzeug.exceptions import NotFound

from ckanext.data_gov_gr import gitbook_print
from ckanext.data_gov_gr import views

GUIDES_URL = 'https://guides.example/guides'


class FakeResponse(object):
    def __init__(self, status_code=200, text='', payload=None):
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError('no json')
        return self._payload


class FakeRedis(object):
    def __init__(self):
        self.store = {}
        self.ttls = {}

    def get(self, key):
        return self.store.get(key)

    def setex(self, key, ttl, value):
        self.store[key] = value
        self.ttls[key] = ttl


def _llms_part(number, total, body, next_url=None):
    header = [
        f'> This is part {number} of {total} of the full documentation (pages 1–100 of 150).',
        '> The content is paginated: fetch every part to see all of it.',
    ]
    if next_url:
        header.append(f'> Next part: {next_url}')
    header.append(f'> Page index: {GUIDES_URL}/llms.txt')
    return '\n'.join(header) + '\n\n' + body


def _fake_get(responses, calls):
    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        response = responses[url]
        if isinstance(response, Exception):
            raise response
        return response
    return fake_get


# --- llms-full.txt -----------------------------------------------------------

def test_fetch_llms_full_follows_parts_and_strips_headers(monkeypatch):
    calls = []
    responses = {
        # Το υποσέλιδο «Next Page» στο τέλος του κομματιού δεν είναι περιεχόμενο
        f'{GUIDES_URL}/llms-full.txt': FakeResponse(text=_llms_part(
            1, 2, f'# Σελίδα 1\n\nΚείμενο 1\n\n\n---\n\n[Next Page]({GUIDES_URL}/llms-full.txt/1)\n\n',
            next_url=f'{GUIDES_URL}/llms-full.txt/1')),
        f'{GUIDES_URL}/llms-full.txt/1': FakeResponse(text=_llms_part(
            2, 2, '# Σελίδα 2\n\nΚείμενο 2')),
    }
    monkeypatch.setattr(gitbook_print.requests, 'get', _fake_get(responses, calls))

    text = gitbook_print.fetch_llms_full(GUIDES_URL + '/')

    assert [url for url, _ in calls] == list(responses)
    assert text == '# Σελίδα 1\n\nΚείμενο 1\n\n# Σελίδα 2\n\nΚείμενο 2'


def test_fetch_llms_full_fails_on_next_part_on_other_host(monkeypatch):
    calls = []
    responses = {
        f'{GUIDES_URL}/llms-full.txt': FakeResponse(text=_llms_part(
            1, 2, '# Σελίδα 1', next_url='https://evil.example/llms-full.txt/1')),
    }
    monkeypatch.setattr(gitbook_print.requests, 'get', _fake_get(responses, calls))

    assert gitbook_print.fetch_llms_full(GUIDES_URL) is None
    assert len(calls) == 1


def test_fetch_llms_full_follows_relative_next_part(monkeypatch):
    responses = {
        f'{GUIDES_URL}/llms-full.txt': FakeResponse(text=_llms_part(
            1, 2, '# Σελίδα 1', next_url='/guides/llms-full.txt/1')),
        f'{GUIDES_URL}/llms-full.txt/1': FakeResponse(text=_llms_part(2, 2, '# Σελίδα 2')),
    }
    monkeypatch.setattr(gitbook_print.requests, 'get', _fake_get(responses, []))

    assert gitbook_print.fetch_llms_full(GUIDES_URL) == '# Σελίδα 1\n\n# Σελίδα 2'


def test_fetch_llms_full_fails_beyond_max_parts(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return FakeResponse(text=_llms_part(
            len(calls), 99, f'# Σελίδα {len(calls)}', next_url=f'{GUIDES_URL}/llms-full.txt/{len(calls)}'))

    monkeypatch.setattr(gitbook_print, 'LLMS_FULL_MAX_PARTS', 2)
    monkeypatch.setattr(gitbook_print.requests, 'get', fake_get)

    # Δεν επιστρέφει κομμένο οδηγό
    assert gitbook_print.fetch_llms_full(GUIDES_URL) is None
    assert len(calls) == 2


@pytest.mark.parametrize('second_part', [
    FakeResponse(status_code=404, text='not found'),
    requests.ConnectionError('down'),
])
def test_fetch_llms_full_returns_none_when_a_part_fails(monkeypatch, second_part):
    responses = {
        f'{GUIDES_URL}/llms-full.txt': FakeResponse(text=_llms_part(
            1, 2, '# Σελίδα 1', next_url=f'{GUIDES_URL}/llms-full.txt/1')),
        f'{GUIDES_URL}/llms-full.txt/1': second_part,
    }
    monkeypatch.setattr(gitbook_print.requests, 'get', _fake_get(responses, []))

    assert gitbook_print.fetch_llms_full(GUIDES_URL) is None


def test_split_llms_header_keeps_text_without_header():
    assert gitbook_print._split_llms_header('> Απλό quote\n\nΚείμενο') == ('', '> Απλό quote\n\nΚείμενο')


# --- GitBook API markdown ------------------------------------------------------

def test_fetch_page_markdown_reads_markdown_and_strips_front_matter(monkeypatch):
    calls = []
    url = 'https://api.gitbook.com/v1/spaces/SPACE/content/page/PAGE'
    responses = {url: FakeResponse(payload={
        'id': 'PAGE',
        'markdown': '---\ndescription: Περιγραφή\n---\n# Τίτλος\n\nΚείμενο',
    })}
    monkeypatch.setattr(gitbook_print.requests, 'get', _fake_get(responses, calls))

    assert gitbook_print.fetch_page_markdown('SPACE', 'TOKEN', 'PAGE') == '# Τίτλος\n\nΚείμενο'
    _, kwargs = calls[0]
    assert kwargs['params'] == {'format': 'markdown'}
    assert kwargs['headers']['Authorization'] == 'Bearer TOKEN'


@pytest.mark.parametrize('response', [
    FakeResponse(status_code=403, payload={'error': {'code': 403, 'type': 'feature-not-available'}},
                 text='{"error":{"code":403}}'),
    FakeResponse(payload={'id': 'PAGE'}),
    FakeResponse(text='not json'),
])
def test_fetch_page_markdown_returns_none_on_bad_response(monkeypatch, response):
    url = 'https://api.gitbook.com/v1/spaces/SPACE/content/page/PAGE'
    monkeypatch.setattr(gitbook_print.requests, 'get', _fake_get({url: response}, []))

    assert gitbook_print.fetch_page_markdown('SPACE', 'TOKEN', 'PAGE') is None


# --- markdown -> HTML ---------------------------------------------------------

def test_render_converts_gitbook_blocks():
    text = '\n'.join([
        '# Οδηγός',
        '',
        '{% hint style="warning" %}',
        '**Προσοχή** εδώ',
        '{% endhint %}',
        '',
        '{% tabs %}',
        '{% tab title="Python" %}',
        'Κείμενο tab',
        '{% endtab %}',
        '{% endtabs %}',
        '',
        '{% stepper %}',
        '{% step %}',
        'Βήμα ένα',
        '{% endstep %}',
        '{% endstepper %}',
        '',
        '{% embed url="<https://guides.example/guides/api>" %}',
        '',
        'Γραμμή ένα\\',
        'Γραμμή δύο',
        '',
        '<details>',
        '',
        '<summary><strong>Ερώτηση;</strong></summary>',
        '',
        'Απάντηση *με* markdown',
        '',
        '</details>',
        '',
        '```',
        '{% raw %} μένει όπως είναι',
        '```',
    ])

    html = gitbook_print.render_gitbook_markdown(text)

    assert '<div class="hint hint-warning">' in html
    assert '<strong>Προσοχή</strong>' in html
    assert '<p class="tab-title"><strong>Python</strong></p>' in html
    assert 'Βήμα ένα' in html and '{% step' not in html
    assert '<a href="https://guides.example/guides/api">' in html
    assert 'Γραμμή ένα<br>' in html
    assert '<details open>' in html and '<em>με</em>' in html
    assert '{% raw %} μένει όπως είναι' in html


def test_render_handles_gitbook_escapes_and_email_links():
    html = gitbook_print.render_gitbook_markdown('\n'.join([
        'Το https\\:// URL, A \\& B, \\<org-slug> και `κώδικας \\& μένει`',
        '',
        'μετατροπή.\\ <br>',
        '',
        'Email στο <opendata@example.gr>.',
    ]), GUIDES_URL + '/')

    assert 'Το https:// URL, A &amp; B, &lt;org-slug&gt; και <code>κώδικας \\&amp; μένει</code>' in html
    assert 'μετατροπή.<br><br>' in html
    # Το mailto βγαίνει κωδικοποιημένο σε entities και δεν γίνεται σχετικό link των οδηγών
    assert 'href="&#109;&#97;&#105;&#108;&#116;&#111;&#58;' in html
    assert 'guides.example/guides/&#109;' not in html


def test_render_cleans_autolinks_from_llms_full():
    html = gitbook_print.render_gitbook_markdown('\n'.join([
        'Άδεια <http://publications.europa.eu/licence/CC\\_BY\\_4\\_0>',
        'url="<https://remote.example.org"\\\\>',
        'frequency="WEEKLY"',
    ]))

    assert '<a href="http://publications.europa.eu/licence/CC_BY_4_0">http://publications.europa.eu/licence/CC_BY_4_0</a>' in html
    assert 'url="<a href="https://remote.example.org">https://remote.example.org</a>"<br>' in html


def _h1(html):
    return re.findall(r'<h1 id="([^"]*)">([^<]*)</h1>', html)


def test_render_gives_unique_greek_ids_to_headings():
    html = gitbook_print.render_gitbook_markdown(
        '# Φορείς & Χρήστες\n\n## Υποενότητα\n\n# Εισαγωγικά\n\n# Εισαγωγικά\n')

    assert _h1(html) == [
        ('φορείς-χρήστες', 'Φορείς &amp; Χρήστες'),
        ('εισαγωγικά', 'Εισαγωγικά'),
        ('εισαγωγικά_1', 'Εισαγωγικά'),
    ]


def test_render_sanitizes_html():
    html = gitbook_print.render_gitbook_markdown('\n'.join([
        'Κείμενο <script>alert(1)</script> και `/api/<org-slug>`',
        '',
        '<img src="https://files.example/a.png" onerror="alert(1)" width="300">',
        '',
        '[κακό](javascript:alert(1))',
        '',
        '<figure><img src="https://files.example/b.png" alt=""><figcaption>Λεζάντα</figcaption></figure>',
    ]))

    assert '<script>' not in html and '&lt;script&gt;' in html
    assert 'onerror' not in html
    assert 'javascript:' not in html
    assert '<code>/api/&lt;org-slug&gt;</code>' in html
    assert '<figure><img src="https://files.example/b.png" alt=""><figcaption>Λεζάντα</figcaption></figure>' in html


def test_render_turns_cards_table_into_list_without_hidden_columns():
    table = (
        '<table data-view="cards"><thead><tr><th></th><th data-type="content-ref"></th>'
        '<th data-hidden data-card-cover data-type="image"></th></tr></thead><tbody>'
        '<tr><td></td><td><a href="/guides/eisagogika">Εισαγωγικά</a></td>'
        '<td><a href="https://images.example/cover.jpg">cover</a></td></tr>'
        '</tbody></table>'
    )

    html = gitbook_print.render_gitbook_markdown(table, GUIDES_URL + '/')

    assert html == (
        '<ul class="cards"><li><a href="https://guides.example/guides/eisagogika">'
        'Εισαγωγικά</a></li></ul>'
    )


def test_render_cards_table_uses_hidden_card_target_as_link():
    table = (
        '<table data-view="cards"><thead><tr><th></th>'
        '<th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody>'
        '<tr><td><strong>Γενικά</strong></td><td><a href="/guides/syxnes-erotiseis/genika">Γενικά</a></td></tr>'
        '</tbody></table>'
    )

    html = gitbook_print.render_gitbook_markdown(table, GUIDES_URL + '/')

    assert html == (
        '<ul class="cards"><li><a href="https://guides.example/guides/syxnes-erotiseis/genika">'
        '<strong>Γενικά</strong></a></li></ul>'
    )


def test_render_absolutizes_only_real_tags_and_escapes_url():
    html = gitbook_print.render_gitbook_markdown(
        '[σχετικό](x) και `<a href="rel">`', 'https://g.example/guides" onmouseover="alert(1)/')

    assert 'onmouseover="' not in html
    assert 'href="https://g.example/guides&quot; onmouseover=&quot;alert(1)/x"' in html
    assert '<code>&lt;a href="rel"&gt;</code>' in html


def test_render_absolutizes_relative_urls_against_page_url():
    html = gitbook_print.render_gitbook_markdown(
        '[σχετικό](leksilogia/access-rights) [απόλυτο](/guides/x) [άγκυρα](#top) '
        '![εικόνα](../assets/a.png)',
        'https://guides.example/guides/eisagogika/leksilogia',
    )

    assert 'href="https://guides.example/guides/eisagogika/leksilogia/access-rights"' in html
    assert 'href="https://guides.example/guides/x"' in html
    assert 'href="#top"' in html
    assert 'src="https://guides.example/guides/assets/a.png"' in html


# --- δέντρο σελίδων ------------------------------------------------------------

def test_render_nests_gitbook_lists():
    # Το GitBook στοιχίζει με 2/3 κενά· το Python-Markdown θέλει 4 ανά επίπεδο
    text = '1. Βήμα ένα\n\n   * α\n     * β\n\n2. Βήμα δύο\n\n   Κείμενο\n3. Βήμα τρία'

    html = gitbook_print.render_gitbook_markdown(text)

    assert html.count('<ol') == 1
    assert re.search(r'<li>α<ul>\s*<li>β</li>', html)
    assert re.search(r'<p>Κείμενο</p>\s*</li>\s*<li>\s*<p>Βήμα τρία</p>', html)
    assert '*' not in html


def test_render_closes_sublist_before_next_numbered_item():
    # Όπως στη σελίδα «Δημιουργία λεξιλογίου»: το βήμα 3 αμέσως μετά από υπο-λίστα
    # που αρχίζει μετά από κενή γραμμή
    text = ('2. **Όνομα**\n\n   * Περιγραφή\n   * Παράδειγμα: `Λεξιλόγιο`\n'
            '3. **Αποθήκευση**\n   * Κάντε κλικ')

    html = gitbook_print.render_gitbook_markdown(text)

    assert '3.' not in html
    assert html.count('<li>') == 5
    assert re.search(r'</ul>\s*</li>\s*<li>\s*<p><strong>Αποθήκευση</strong></p>\s*<ul>\s*<li>Κάντε κλικ', html)


def test_render_keeps_tight_list_after_sublist():
    html = gitbook_print.render_gitbook_markdown('* α\n  * β\n* γ')

    assert '<p>' not in html
    assert re.search(r'</ul>\s*</li>\s*<li>γ</li>', html)


def test_render_keeps_hint_in_list_as_block_and_continues_numbering():
    text = '1. Ένα\n\n   {% hint style="info" %}\n   Σημείωση\n   {% endhint %}\n\n2. Δύο'

    html = gitbook_print.render_gitbook_markdown(text)

    assert re.search(r'<div class="hint hint-info">\s*<p>Σημείωση</p>\s*</div>', html)
    assert '<ol start="2">' in html


def test_render_does_not_reindent_lists_inside_code():
    html = gitbook_print.render_gitbook_markdown('Κείμενο:\n* α\n* β\n\n```\n  * όχι λίστα\n```')

    assert '<ul>\n<li>α</li>\n<li>β</li>\n</ul>' in html
    assert '<pre><code>  * όχι λίστα\n</code></pre>' in html


def _page(page_id, title, pages=None, **flags):
    return dict({'id': page_id, 'title': title, 'description': None, 'type': 'document',
                 'path': page_id.lower(), 'hidden': None, 'noIndex': None, 'pages': pages or []}, **flags)


SPACE_PAGES = [
    _page('INTRO', 'Εισαγωγικά'),
    _page('LEGAL', 'Νομικός Οδηγός', [
        _page('LAW', 'Νόμος 4727/2020', description='Ο νόμος για την ψηφιακή διακυβέρνηση', pages=[
            _page('ART', 'Άρθρο 63'),
            _page('DRAFT', 'Πρόχειρο', hidden=True),
        ]),
        _page('SECRET', 'Κρυφή ενότητα', [_page('SECRET_CHILD', 'Παιδί κρυφής')], hidden=True),
        _page('NOINDEX', 'Χωρίς ευρετήριο', noIndex=True),
    ]),
    _page('HIDDEN_ROOT', 'Κρυφή ρίζα', hidden=True),
]


def test_select_pages_skips_hidden_pages():
    selected = gitbook_print.select_pages(SPACE_PAGES)

    assert [p['id'] for p in selected] == ['INTRO', 'LEGAL', 'LAW', 'ART', 'NOINDEX']


def test_split_sections_ignores_headings_inside_code():
    text = '# Πρώτη\n\nΚείμενο\n\n```bash\n# σχόλιο\n```\n\n# Δεύτερη\n\nΤέλος'

    assert gitbook_print.split_sections(text) == [
        ('Πρώτη', '# Πρώτη\n\nΚείμενο\n\n```bash\n# σχόλιο\n```'),
        ('Δεύτερη', '# Δεύτερη\n\nΤέλος'),
    ]


def _llms_text(titles):
    return '\n\n'.join(f'# {title}\n\nΚείμενο {title}' for title in titles)


def test_fetch_llms_sections_aligns_pages_without_noindex(monkeypatch):
    # Το llms-full.txt περιέχει και κρυφές σελίδες, όχι όμως τις noIndex
    titles = ['Εισαγωγικά', 'Νομικός Οδηγός', 'Νόμος 4727/2020', 'Άρθρο 63', 'Πρόχειρο',
              'Κρυφή ενότητα', 'Παιδί κρυφής', 'Κρυφή ρίζα']
    monkeypatch.setattr(gitbook_print, 'fetch_llms_full', lambda url: _llms_text(titles))

    result = gitbook_print.fetch_llms_sections(GUIDES_URL, SPACE_PAGES)

    assert list(result) == ['INTRO', 'LEGAL', 'LAW', 'ART', 'DRAFT', 'SECRET', 'SECRET_CHILD', 'HIDDEN_ROOT']
    assert result['ART'] == '# Άρθρο 63\n\nΚείμενο Άρθρο 63'


def test_fetch_llms_sections_reports_mismatch(monkeypatch):
    monkeypatch.setattr(gitbook_print, 'fetch_llms_full', lambda url: _llms_text(['Άλλο', 'Κάτι']))

    assert gitbook_print.fetch_llms_sections(GUIDES_URL, SPACE_PAGES) is None


def _patch_build(monkeypatch, llms, api_markdown):
    monkeypatch.setattr(gitbook_print, 'fetch_space', lambda space_id, token: {
        'title': 'Οδηγοί και τεκμηρίωση', 'pages': SPACE_PAGES,
    })
    monkeypatch.setattr(gitbook_print, 'fetch_llms_sections', lambda url, pages: llms)
    fetched = []

    def fake_fetch(space_id, token, page_id):
        fetched.append(page_id)
        return api_markdown.get(page_id)

    monkeypatch.setattr(gitbook_print, 'fetch_page_markdown', fake_fetch)
    return fetched


LLMS_BY_ID = {
    'INTRO': '# Εισαγωγικά',
    'LEGAL': '# Νομικός Οδηγός',
    'LAW': '# Νόμος 4727/2020\n\nΟ νόμος για την ψηφιακή διακυβέρνηση\n\n[Άρθρο](/guides/legal/law/art)',
    'ART': '# Άρθρο 63\n\nΚείμενο άρθρου',
}


def test_build_document_uses_llms_and_fetches_noindex_pages(monkeypatch):
    fetched = _patch_build(monkeypatch, LLMS_BY_ID, {
        'NOINDEX': '# Χωρίς ευρετήριο\n\n[σχετικό](other)',
    })

    document = gitbook_print.build_document('SPACE', 'TOKEN', GUIDES_URL + '/')

    assert fetched == ['NOINDEX']
    assert document['title'] == 'Οδηγοί και τεκμηρίωση'
    assert [title for _, title in _h1(document['html'])] == [
        'Εισαγωγικά', 'Νομικός Οδηγός', 'Νόμος 4727/2020', 'Άρθρο 63', 'Χωρίς ευρετήριο',
    ]
    assert 'href="https://guides.example/guides/legal/law/art"' in document['html']
    # Τα σχετικά links του API λύνονται ως προς το path της σελίδας τους
    assert 'href="https://guides.example/guides/other"' in document['html']
    assert 'Πρόχειρο' not in document['html']
    assert ('<h1 id="νόμος-47272020">Νόμος 4727/2020</h1>\n'
            '<p class="page-description">Ο νόμος για την ψηφιακή διακυβέρνηση</p>') in document['html']
    assert document['html'].count('Ο νόμος για την ψηφιακή διακυβέρνηση') == 1


def test_build_document_without_aligned_llms_fails_without_api_calls(monkeypatch):
    fetched = _patch_build(monkeypatch, None, {})

    assert gitbook_print.build_document('SPACE', 'TOKEN', GUIDES_URL) is None
    assert fetched == []


def test_build_document_returns_none_when_a_page_cannot_be_fetched(monkeypatch):
    _patch_build(monkeypatch, LLMS_BY_ID, {})

    assert gitbook_print.build_document('SPACE', 'TOKEN', GUIDES_URL) is None


def test_style_description_adds_description_from_api_markdown():
    # Το markdown του API δεν έχει την περιγραφή· μπαίνει κάτω από τον τίτλο
    assert gitbook_print._style_description('# Τίτλος\n\nΚείμενο', 'Περιγραφή <b>') == (
        '# Τίτλος\n\n<p class="page-description">Περιγραφή &lt;b&gt;</p>\n\nΚείμενο'
    )
    assert gitbook_print._style_description('# Τίτλος\n\nΚείμενο', None) == '# Τίτλος\n\nΚείμενο'


def test_style_description_matches_escaped_llms_paragraph():
    section = '# Τίτλος\n\nΟδηγός για CC\\_BY \\& άλλα\n\nΚείμενο'

    assert gitbook_print._style_description(section, 'Οδηγός για CC_BY & άλλα') == (
        '# Τίτλος\n\n<p class="page-description">Οδηγός για CC_BY &amp; άλλα</p>\n\nΚείμενο'
    )


def test_fetch_space_fails_when_space_info_fails(monkeypatch):
    monkeypatch.setattr(gitbook_print, '_api_get_json', lambda url, token, params=None: (
        {'pages': []} if url.endswith('/content/pages') else None
    ))

    assert gitbook_print.fetch_space('SPACE', 'TOKEN') is None


def test_get_settings_requires_space_token_and_guides_url(monkeypatch):
    settings = {
        'ckanext.data_gov_gr.gitbook.space_id': ' SPACE ',
        'ckanext.data_gov_gr.gitbook.api_token': 'TOKEN',
        'guides_base_url': GUIDES_URL,
    }
    monkeypatch.setattr(gitbook_print, 'config', settings)
    assert gitbook_print.get_settings() == ('SPACE', 'TOKEN', GUIDES_URL)

    monkeypatch.setattr(gitbook_print, 'config', dict(settings, guides_base_url=''))
    assert gitbook_print.get_settings() is None


def test_get_document_key_includes_render_version_space_and_guides(monkeypatch):
    keys = []
    monkeypatch.setattr(gitbook_print, 'get_cached', lambda key, producer: keys.append(key))

    gitbook_print.get_document('SPACE', 'TOKEN', GUIDES_URL)

    url_hash = gitbook_print.url_hash(GUIDES_URL)
    assert keys == [f'gitbook_print:v{gitbook_print.RENDER_VERSION}:SPACE:{url_hash}']


# --- Redis cache ----------------------------------------------------------------

def test_get_cached_stores_value_and_skips_failures(monkeypatch):
    import ckan.lib.redis

    redis = FakeRedis()
    monkeypatch.setattr(ckan.lib.redis, 'connect_to_redis', lambda: redis)
    monkeypatch.setattr(gitbook_print, 'config', {
        'ckan.site_id': 'site', gitbook_print.CACHE_TTL_KEY: 60,
    })
    produced = []

    def producer():
        produced.append(1)
        return {'title': 'Οδηγοί', 'html': '<p>Γεια</p>'}

    assert gitbook_print.get_cached('key', producer) == {'title': 'Οδηγοί', 'html': '<p>Γεια</p>'}
    assert gitbook_print.get_cached('key', producer) == {'title': 'Οδηγοί', 'html': '<p>Γεια</p>'}
    assert len(produced) == 1
    assert redis.ttls == {'ckan:site:ckanext:data_gov_gr:key': 60}

    assert gitbook_print.get_cached('failed', lambda: None) is None
    assert 'ckan:site:ckanext:data_gov_gr:failed' not in redis.store


def test_get_cached_works_without_redis(monkeypatch):
    import ckan.lib.redis

    def broken_redis():
        raise ConnectionError('no redis')

    monkeypatch.setattr(ckan.lib.redis, 'connect_to_redis', broken_redis)

    assert gitbook_print.get_cached('key', lambda: [1, 2]) == [1, 2]


def test_get_cached_uses_default_ttl_when_setting_is_invalid(monkeypatch):
    import ckan.lib.redis

    redis = FakeRedis()
    monkeypatch.setattr(ckan.lib.redis, 'connect_to_redis', lambda: redis)
    monkeypatch.setattr(gitbook_print, 'config', {gitbook_print.CACHE_TTL_KEY: '1d'})

    assert gitbook_print.get_cached('key', lambda: [1]) == [1]
    assert list(redis.ttls.values()) == [gitbook_print.CACHE_TTL_DEFAULT]


def test_get_cached_uses_default_ttl_when_setting_is_empty(monkeypatch):
    # «pages_cache_ttl =» χωρίς τιμή στο ini δεν κλείνει την cache
    import ckan.lib.redis

    redis = FakeRedis()
    monkeypatch.setattr(ckan.lib.redis, 'connect_to_redis', lambda: redis)
    monkeypatch.setattr(gitbook_print, 'config', {gitbook_print.CACHE_TTL_KEY: ' '})

    assert gitbook_print.get_cached('key', lambda: [1]) == [1]
    assert list(redis.ttls.values()) == [gitbook_print.CACHE_TTL_DEFAULT]


# --- views ------------------------------------------------------------------------

CREDENTIALS = {
    'guides_base_url': GUIDES_URL,
    'ckanext.data_gov_gr.gitbook.space_id': 'SPACE',
    'ckanext.data_gov_gr.gitbook.api_token': 'TOKEN',
}


@pytest.fixture
def request_context():
    app = flask.Flask(__name__)
    with app.test_request_context('/', environ_base={'CKAN_LANG': 'el'}):
        yield


@pytest.fixture
def rendered(monkeypatch):
    calls = []
    monkeypatch.setattr(views, 'render', lambda template, extra_vars: calls.append((template, extra_vars)) or 'html')
    monkeypatch.setattr(views.gitbook_print, 'config', dict(CREDENTIALS))
    return calls


def _patch_document(monkeypatch, document):
    calls = []
    monkeypatch.setattr(views.gitbook_print, 'get_document', lambda *args: calls.append(args) or document)
    return calls


def test_download_guides_pdf_renders_print_page_with_space_title(monkeypatch, request_context, rendered):
    calls = _patch_document(monkeypatch, {'title': 'Οδηγοί και τεκμηρίωση', 'html': '<h1 id="a">A</h1>'})

    assert views.download_guides_pdf() == 'html'
    assert calls == [('SPACE', 'TOKEN', GUIDES_URL)]
    template, extra_vars = rendered[0]
    assert template == views.GITBOOK_PRINT_TEMPLATE
    assert extra_vars == {
        'title': 'Οδηγοί και τεκμηρίωση', 'content': '<h1 id="a">A</h1>', 'guides_url': GUIDES_URL,
    }


def test_download_guides_pdf_redirects_to_guides_when_build_fails(monkeypatch, request_context, rendered):
    _patch_document(monkeypatch, None)

    response = views.download_guides_pdf()

    assert response.status_code == 302
    assert response.location == GUIDES_URL
    assert rendered == []


def test_download_guides_pdf_requires_credentials(monkeypatch, request_context, rendered):
    monkeypatch.setattr(views.gitbook_print, 'config', {'guides_base_url': GUIDES_URL})

    with pytest.raises(NotFound):
        views.download_guides_pdf()
