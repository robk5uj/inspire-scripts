from invenio.dbquery import run_sql
from invenio.search_engine import search_pattern
from invenio.search_engine_summarizer import generate_citation_summary


def print_author_list(top_authors):
    # for author, h_index in top_authors:
    #     print author, ' -> ', h_index
    print len(top_authors)

def main():
    collaboration_papers = search_pattern(p='20+', f='authorcount')

    min_h_index = 0
    top_500_authors = []

    all_canonical_names = run_sql('SELECT data FROM aidPERSONIDDATA WHERE tag = "canonical_name"')
    for done, (canonical_name, ) in enumerate(all_canonical_names):
        if done % 1000 == 0:
            print 'Done %s' % done
            print_author_list(top_500_authors)
        recids = search_pattern(p=canonical_name, f='author')
        recids -= collaboration_papers
        stats = generate_citation_summary(recids,
                          collections=[('Citeable papers', 'collection:citeable')])
        h_index = stats[1]['Citeable papers']['h-index']
        if h_index > min_h_index or len(top_500_authors) < 500:
            top_500_authors.append((canonical_name, h_index))
            top_500_authors.sort(key=lambda x: x[1], reverse=True)
            top_500_authors = top_500_authors[:500]
            min_h_index = top_500_authors[-1][1]

    print 'Final list'
    print_author_list(top_500_authors)


if __name__ == '__main__':
    main()
