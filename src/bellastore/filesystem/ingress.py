from bellastore.filesystem.base_folder import BaseFolder

class Ingress(BaseFolder):
    def __init__(self, path):
        super().__init__(path)

    