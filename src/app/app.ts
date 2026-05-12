import { Component, inject, signal, computed } from '@angular/core';
import { GithubService } from './services/github.service';
import { GitCommit } from './models/commit.model';
import { SearchComponent } from './components/search/search.component';
import { UniverseComponent } from './components/universe/universe.component';
import { DatePipe } from '@angular/common';

type State = 'search' | 'loading' | 'universe' | 'error';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [SearchComponent, UniverseComponent, DatePipe],
  templateUrl: './app.html',
  styleUrl: './app.scss',
})
export class App {
  private github = inject(GithubService);

  state = signal<State>('search');
  commits = signal<GitCommit[]>([]);
  repoLabel = signal('');
  errorMsg = signal('');
  selectedCommit = signal<GitCommit | null>(null);

  oldestDate = computed(() => {
    const c = this.commits();
    if (!c.length) return null;
    return c.reduce((a, b) => a.date < b.date ? a : b).date;
  });

  newestDate = computed(() => {
    const c = this.commits();
    if (!c.length) return null;
    return c.reduce((a, b) => a.date > b.date ? a : b).date;
  });

  onSearch(input: string): void {
    const parsed = this.github.parseRepo(input);
    if (!parsed) {
      this.state.set('error');
      this.errorMsg.set('No puedo parsear ese repo.');
      return;
    }

    this.state.set('loading');
    this.repoLabel.set(`${parsed.owner}/${parsed.name}`);
    this.commits.set([]);

    this.github.getCommits(parsed.owner, parsed.name).subscribe({
      next: commits => {
        this.commits.set(commits);
        this.state.set('universe');
      },
      error: err => {
        const msg = err.status === 404
          ? 'Repositorio no encontrado.'
          : err.status === 403
            ? 'Límite de API de GitHub alcanzado. Espera un momento.'
            : 'Error al cargar commits.';
        this.errorMsg.set(msg);
        this.state.set('error');
      },
    });
  }

  resetToSearch(): void {
    this.state.set('search');
    this.commits.set([]);
    this.repoLabel.set('');
    this.selectedCommit.set(null);
  }
}
