"""Cluster plugin page routes"""

from litestar import get
from litestar.response import Template

from tusk.studio.routes.base import TuskController


class ClusterPageController(TuskController):
    """Serves cluster HTML pages"""

    path = "/cluster"

    @get("/")
    async def cluster_dashboard(self) -> Template:
        """Cluster dashboard page"""
        return self.render("plugins/cluster/dashboard.html", active_page="cluster")
