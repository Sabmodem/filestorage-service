<template>
  <v-app>
    <v-main>
      <v-container>
        <div class="d-flex flex-column justify-center align-center" action="/api/filestorage/files" enctype="multipart/form-data" method="post">
          <div class="d-flex flex-row w-100">
            <v-file-input name="files" label="загрузить файл" v-model="upload"></v-file-input>
          </div>
          <div class="d-flex w-100">
            <v-btn class="d-flex w-100" type="submit" @click="uploadFiles">отправить</v-btn>
          </div>
        </div>
        <v-data-table-server :items="files" :headers="headers" @update:options="getFilesList" class="mt-3">
          <template v-slot:top>
            <v-toolbar flat>
              <v-toolbar-title>доступные файлы</v-toolbar-title>
              <v-dialog v-model="dialogDelete" max-width="500px">
                <v-card>
                  <v-card-title class="text-h5">Удалить файл?</v-card-title>
                  <v-card-actions>
                    <v-spacer></v-spacer>
                    <v-btn color="blue-darken-1" variant="text" @click="closeDelete">Нет</v-btn>
                    <v-btn color="blue-darken-1" variant="text" @click="deleteItemConfirm">Да</v-btn>
                    <v-spacer></v-spacer>
                  </v-card-actions>
                </v-card>
              </v-dialog>
            </v-toolbar>
          </template>
          <template v-slot:item.actions="{ item }">
            <v-icon size="small" @click="deleteItem(item)">
              mdi-delete
            </v-icon>
          </template>
        </v-data-table-server>
      </v-container>
    </v-main>
  </v-app>
</template>

<script>
import axios from 'axios';

const ai = axios.create({
  headers: { Authorization: `Bearer ${localStorage.getItem('access-token')}` },
});

export default {
  data() {
    return {
      files: [],
      headers: [
        { title: 'название', key: 'filename' },
        { title: 'действия', key: 'actions', sortable: false },
      ],
      dialogDelete: false,
      upload: null
    };
  },
  methods: {
    deleteItem (item) {
      this.editedIndex = this.files.indexOf(item);
      this.editedItem = Object.assign({}, item);
      this.dialogDelete = true;
    },

    async deleteItemConfirm () {
      await ai.delete(`/api/filestorage/files/${this.files[this.editedIndex].filename}`);
      await this.getFilesList();
      this.files.splice(this.editedIndex, 1);
      this.closeDelete();
    },

    closeDelete () {
      this.dialogDelete = false;
      this.$nextTick(() => {
        this.editedItem = Object.assign({}, this.defaultItem);
        this.editedIndex = -1;
      })
    },

    async getFilesList() {
      const files = await ai.get('/api/filestorage/files');
      this.files = files.data;
    },

    async uploadFiles() {
      if (!this.upload) {
        return;
      }

      const formData = new FormData();
      for (let file of this.upload) {
          formData.append("files", file, file.name);
      };

      try {
        await ai.post("/api/filestorage/files", formData);
        alert('Успешно');
        document.location.reload();
      } catch(error) {
        alert('Ошибка');
      };
    }    
  }
}
</script>