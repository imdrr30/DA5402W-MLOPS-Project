import { Component, OnInit, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router, RouterModule } from '@angular/router';
import { ApiService, User } from '../../services/api.service';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterModule],
  templateUrl: './dashboard.html'
})
export class DashboardComponent implements OnInit {
  products = signal<any[]>([]);
  categories = signal<any[]>([]);
  recommendations = signal<any[]>([]);
  
  selectedCategoryId = signal<number | null>(null);
  searchQuery = signal<string>('');
  currentPage = signal<number>(1);
  totalPages = signal<number>(1);
  totalProducts = signal<number>(0);
  pageSize = 12;

  loadingProducts = signal<boolean>(false);
  loadingRecs = signal<boolean>(false);
  isTraining = signal<boolean>(false);
  trainingResults = signal<any | null>(null);
  simStatus = signal<any | null>(null);
  simRunning = signal<boolean>(false);
  simPollInterval: any = null;

  constructor(
    public readonly apiService: ApiService,
    private readonly router: Router
  ) {}

  ngOnInit() {
    this.loadCategories();
    this.loadProducts();
    this.loadRecommendations();
  }

  loadCategories() {
    this.apiService.getCategories().subscribe({
      next: (data) => this.categories.set(data),
      error: (err) => console.error('Failed to load categories', err)
    });
  }

  loadProducts() {
    this.loadingProducts.set(true);
    const catId = this.selectedCategoryId();
    const page = this.currentPage();
    const search = this.searchQuery();

    if (catId !== null) {
      // Load products by category
      this.apiService.getProductsByCategory(catId, page, this.pageSize).subscribe({
        next: (res) => {
          this.products.set(res.products);
          this.totalProducts.set(res.total);
          this.totalPages.set(res.pages);
          this.loadingProducts.set(false);
        },
        error: (err) => {
          console.error('Failed to load category products', err);
          this.loadingProducts.set(false);
        }
      });
    } else {
      // Load all products (searchable)
      this.apiService.getProducts(page, this.pageSize, search).subscribe({
        next: (res) => {
          this.products.set(res.products);
          this.totalProducts.set(res.total);
          this.totalPages.set(res.pages);
          this.loadingProducts.set(false);
        },
        error: (err) => {
          console.error('Failed to load products', err);
          this.loadingProducts.set(false);
        }
      });
    }
  }

  loadRecommendations() {
    this.loadingRecs.set(true);
    this.apiService.getRecommendations(6).subscribe({
      next: (data) => {
        this.recommendations.set(data);
        this.loadingRecs.set(false);
      },
      error: (err) => {
        console.error('Failed to load recommendations', err);
        this.loadingRecs.set(false);
      }
    });
  }

  onCategorySelect(catId: number | null) {
    this.selectedCategoryId.set(catId);
    this.currentPage.set(1);
    this.searchQuery.set('');
    this.loadProducts();
  }

  onSearch() {
    this.selectedCategoryId.set(null);
    this.currentPage.set(1);
    this.loadProducts();
  }

  goToPage(page: number) {
    if (page >= 1 && page <= this.totalPages()) {
      this.currentPage.set(page);
      this.loadProducts();
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }
  }

  addToCart(product: any, isRecommended: boolean = false, event: MouseEvent) {
    event.stopPropagation(); // Avoid navigating to details

    if (!this.apiService.currentUser()) {
      // User must login
      this.router.navigate(['/login']);
      return;
    }

    this.apiService.addToCart(product.id, 1, isRecommended).subscribe({
      next: () => {
        alert(`${product.title.substring(0, 30)}... added to cart!`);
        // Refresh recommendations downstream
        this.loadRecommendations();
      },
      error: (err) => {
        console.error('Error adding to cart', err);
        alert('Failed to add product to cart.');
      }
    });
  }

  logout() {
    this.apiService.logout();
    this.router.navigate(['/login']);
  }

  onTrainModel() {
    this.isTraining.set(true);
    this.trainingResults.set(null);
    this.apiService.trainModel().subscribe({
      next: (res) => {
        this.isTraining.set(false);
        this.trainingResults.set(res);
        // Reload recommendations to load the precomputed model outputs
        this.loadRecommendations();
      },
      error: (err) => {
        this.isTraining.set(false);
        alert('Model training failed: ' + (err.error?.message || err.message));
      }
    });
  }

  onStartSimulation() {
    this.simRunning.set(true);
    this.apiService.startSimulation().subscribe({
      next: () => {
        this.pollSimulationStatus();
      },
      error: (err) => {
        this.simRunning.set(false);
        alert('Simulation failed to start: ' + (err.error?.error || err.message));
      }
    });
  }

  pollSimulationStatus() {
    if (this.simPollInterval) {
      clearInterval(this.simPollInterval);
    }
    
    // Initial fetch
    this.apiService.getSimulationStatus().subscribe(res => this.simStatus.set(res));

    this.simPollInterval = setInterval(() => {
      this.apiService.getSimulationStatus().subscribe({
        next: (res) => {
          this.simStatus.set(res);
          if (res.status === 'completed') {
            clearInterval(this.simPollInterval);
            this.simRunning.set(false);
            alert('Simulation completed successfully! Retraining models now...');
            this.onTrainModel();
          } else if (res.status === 'error') {
            clearInterval(this.simPollInterval);
            this.simRunning.set(false);
            alert('Simulation failed: ' + res.message);
          }
        },
        error: (err) => {
          console.error('Error polling simulation status', err);
        }
      });
    }, 1000);
  }
}
