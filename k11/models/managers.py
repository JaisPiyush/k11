from mongoengine import QuerySet
from mongoengine.queryset.visitor import Q


class SourceMapQuerySet(QuerySet):

    def pull_all_rss_models(self):
        return self.filter(Q(is_rss=True) & Q(is_collection=True) & Q(is_third_party=False))
    
    def pull_all_html_models(self):
        return self.filter(Q(is_rss=False) & Q(is_collection=True) & Q(is_third_party=False))
    
    def pull_all_youtube_collections(self):
        return self.filter(Q(source_id='youtube') & Q(is_third_party=True))


class DatalinkContainerQuerySet(QuerySet):

    def delete_containers(self, links: str):
        self.filter(Q(link__in=links) & Q(is_transient=True)).delete()
    
    def is_article_present_in_db(self, link):
        return self.filter(link=link).count() > 0


class FormatsQuerySet(QuerySet):
    def get_default_rss_format(self):
        return self.get(format_id="default_rss_format")